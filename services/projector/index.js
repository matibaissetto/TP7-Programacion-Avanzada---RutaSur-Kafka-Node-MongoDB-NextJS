const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const kafka = new Kafka({
  clientId: 'rutasur-projector',
  brokers: ['localhost:29092'],
});

const consumer = kafka.consumer({
  groupId: 'proyector-v2',
});

const producer = kafka.producer();

const mongo = new MongoClient(
  'mongodb://localhost:27017',
);

async function enviarDLQ(message, motivo) {
  await producer.send({
    topic: 'envios.estados.dlq',
    messages: [
      {
        key: 'invalid',
        value: JSON.stringify({
          motivo,
          recibidoEn: new Date().toISOString(),
          original: message,
        }),
      },
    ],
  });

  console.log(`DLQ -> ${motivo}`);
}

async function main() {
  await mongo.connect();

  const db = mongo.db('rutasur');
  const envios = db.collection('envios');
  const procesados = db.collection('eventos_procesados');

  await envios.createIndex(
    { shipmentId: 1 },
    { unique: true },
  );

  await procesados.createIndex(
    { consumidor: 1, eventId: 1 },
    { unique: true },
  );

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({
    topic: 'envios.estados',
    fromBeginning: true,
  });

  console.log('Projector escuchando envios.estados...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      let event;

      try {
        event = JSON.parse(message.value.toString());
      } catch {
        await enviarDLQ(
          message.value.toString(),
          'JSON_INVALIDO',
        );
        return;
      }

      if (
        !event.eventId ||
        !event.shipmentId ||
        !event.tipo ||
        event.secuencia === undefined
      ) {
        await enviarDLQ(
          event,
          'ESQUEMA_INVALIDO',
        );
        return;
      }

      const duplicado = await procesados.findOne({
        consumidor: 'proyector',
        eventId: event.eventId,
      });

      if (duplicado) {
        console.log(
          `Duplicado descartado: ${event.eventId}`,
        );
        return;
      }

      const envio = await envios.findOne({
        shipmentId: event.shipmentId,
      });

      if (
        envio &&
        event.secuencia <=
          (envio.ultimaSecuencia ?? 0)
      ) {
        console.log(
          `Fuera de orden descartado: ${event.shipmentId}`,
        );

        await procesados.insertOne({
          consumidor: 'proyector',
          eventId: event.eventId,
          fecha: new Date(),
        });

        return;
      }

      await envios.updateOne(
        {
          shipmentId: event.shipmentId,
        },
        {
          $set: {
            estado: event.tipo,
            hubId: event.hubId,
            zonaId: event.zonaId,
            repartidorId: event.repartidorId,
            ultimaSecuencia: event.secuencia,
            actualizadoEn: new Date(),
          },
          $push: {
            historial: {
              tipo: event.tipo,
              ocurridoEn: event.ocurridoEn,
              secuencia: event.secuencia,
              motivo: event.motivo ?? null,
            },
          },
        },
        {
          upsert: true,
        },
      );

      await procesados.insertOne({
        consumidor: 'proyector',
        eventId: event.eventId,
        fecha: new Date(),
      });

      console.log(
        `${event.shipmentId} -> ${event.tipo}`,
      );
    },
  });
}

main().catch(console.error);
