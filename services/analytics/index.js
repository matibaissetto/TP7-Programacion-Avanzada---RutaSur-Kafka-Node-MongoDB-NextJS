const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const kafka = new Kafka({
  clientId: 'rutasur-analytics',
  brokers: ['localhost:29092'],
});

const consumer = kafka.consumer({
  groupId: 'analitica',
});

const mongo = new MongoClient('mongodb://localhost:27017');

function obtenerFranja(fechaIso) {
  const hora = new Date(fechaIso).getHours();
  return hora < 12 ? 'MANANA' : 'TARDE';
}

async function main() {
  await mongo.connect();

  const db = mongo.db('rutasur');
  const metricas = db.collection('metricas_diarias');

  await metricas.createIndex(
    {
      fecha: 1,
      zonaId: 1,
      hubId: 1,
      franja: 1,
      motivo: 1,
    },
    { unique: true },
  );

  await consumer.connect();

  await consumer.subscribe({
    topic: 'envios.estados',
    fromBeginning: true,
  });

  console.log('Analytics escuchando envios.estados...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());

      const fecha = event.ocurridoEn.slice(0, 10);
      const franja = obtenerFranja(event.ocurridoEn);

      const base = {
        fecha,
        zonaId: event.zonaId ?? 'SIN_ZONA',
        hubId: event.hubId ?? 'SIN_HUB',
        franja,
        motivo: event.motivo ?? 'NINGUNO',
      };

      const incremento = {
        totalEventos: 1,
      };

      if (event.tipo === 'ENTREGADO') {
        incremento.entregados = 1;
      }

      if (event.tipo === 'ENTREGA_FALLIDA') {
        incremento.fallos = 1;
      }

      await metricas.updateOne(
        base,
        {
          $inc: incremento,
        },
        {
          upsert: true,
        },
      );

      console.log(
        `${event.shipmentId} -> ANALYTICS -> ${event.tipo}`,
      );
    },
  });
}

main().catch(console.error);