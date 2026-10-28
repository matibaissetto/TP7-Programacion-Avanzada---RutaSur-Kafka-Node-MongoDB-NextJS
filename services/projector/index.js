const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const kafka = new Kafka({
  clientId: 'rutasur-projector',
  brokers: [process.env.KAFKA_BROKER ?? 'localhost:29092'],
});

const consumer = kafka.consumer({
  groupId: 'proyector-v3',
});

const producer = kafka.producer();

const mongo = new MongoClient(
  process.env.MONGO_URI ?? 'mongodb://localhost:27017'
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

async function procesarEstado(db, event) {
  const envios = db.collection('envios');
  const procesados = db.collection('eventos_procesados');

  if (
    !event.eventId ||
    !event.shipmentId ||
    !event.tipo ||
    event.secuencia === undefined
  ) {
    await enviarDLQ(event, 'ESQUEMA_INVALIDO');
    return;
  }

  const duplicado = await procesados.findOne({
    consumidor: 'proyector',
    eventId: event.eventId,
  });

  if (duplicado) {
    console.log(`Duplicado descartado: ${event.eventId}`);
    return;
  }

  const envio = await envios.findOne({
    shipmentId: event.shipmentId,
  });

  if (
    envio &&
    event.secuencia <= (envio.ultimaSecuencia ?? 0)
  ) {
    console.log(
      `Fuera de orden descartado: ${event.shipmentId}`
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
        shipmentId: event.shipmentId,
        estado: event.tipo,
        hubId: event.hubId ?? null,
        zonaId: event.zonaId ?? null,
        repartidorId: event.repartidorId ?? null,
        ultimaSecuencia: event.secuencia,
        actualizadoEn: new Date(),
      },
      $push: {
        historial: {
          eventId: event.eventId,
          tipo: event.tipo,
          ocurridoEn: event.ocurridoEn,
          secuencia: event.secuencia,
          motivo: event.motivo ?? null,
        },
      },
    },
    {
      upsert: true,
    }
  );

  await procesados.insertOne({
    consumidor: 'proyector',
    eventId: event.eventId,
    fecha: new Date(),
  });

  console.log(`${event.shipmentId} -> ${event.tipo}`);
}

async function procesarNotificacion(db, event) {
  await db.collection('notificaciones').updateOne(
    { eventId: event.eventId },
    { $setOnInsert: event },
    { upsert: true }
  );

  await db.collection('envios').updateOne(
    { shipmentId: event.shipmentId },
    {
      $push: {
        notificaciones: event,
      },
      $set: {
        actualizadoEn: new Date(),
      },
    }
  );

  console.log(`${event.shipmentId} -> NOTIFICACION PROYECTADA`);
}

async function procesarReprogramacion(db, event) {
  await db.collection('reprogramaciones').updateOne(
    { eventId: event.eventId },
    { $setOnInsert: event },
    { upsert: true }
  );

  await db.collection('envios').updateOne(
    { shipmentId: event.shipmentId },
    {
      $push: {
        reprogramaciones: event,
      },
      $set: {
        actualizadoEn: new Date(),
      },
    }
  );

  console.log(`${event.shipmentId} -> REPROGRAMACION PROYECTADA`);
}

async function procesarAlerta(db, event) {
  await db.collection('alertas').updateOne(
    { eventId: event.eventId },
    {
      $set: {
        ...event,
        actualizadoEn: new Date(),
      },
    },
    { upsert: true }
  );

  await db.collection('envios').updateOne(
    { shipmentId: event.shipmentId },
    {
      $push: {
        alertas: event,
      },
      $set: {
        actualizadoEn: new Date(),
      },
    }
  );

  console.log(`${event.shipmentId} -> ALERTA PROYECTADA`);
}

async function main() {
  await mongo.connect();

  const db = mongo.db('rutasur');

  await db.collection('envios').createIndex(
    { shipmentId: 1 },
    { unique: true }
  );

  await db.collection('eventos_procesados').createIndex(
    { consumidor: 1, eventId: 1 },
    { unique: true }
  );

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({
    topics: [
      'envios.estados',
      'envios.reprogramaciones',
      'notificaciones.enviadas',
      'alertas.sla',
    ],
    fromBeginning: true,
  });

  console.log('Projector escuchando todos los eventos...');

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      let event;

      try {
        event = JSON.parse(message.value.toString());
      } catch {
        if (topic === 'envios.estados') {
          await enviarDLQ(
            message.value.toString(),
            'JSON_INVALIDO'
          );
        }
        return;
      }

      if (topic === 'envios.estados') {
        await procesarEstado(db, event);
      }

      if (topic === 'notificaciones.enviadas') {
        await procesarNotificacion(db, event);
      }

      if (topic === 'envios.reprogramaciones') {
        await procesarReprogramacion(db, event);
      }

      if (topic === 'alertas.sla') {
        await procesarAlerta(db, event);
      }
    },
  });
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});