const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');
const { randomUUID } = require('crypto');

const kafka = new Kafka({
  clientId: 'rutasur-sla-monitor',
  brokers: [process.env.KAFKA_BROKER ?? 'localhost:29092'],
});

const consumer = kafka.consumer({
  groupId: 'monitor-sla-v2',
});

const producer = kafka.producer();

const mongoUri =
  process.env.MONGO_URI ??
  (process.env.KAFKA_BROKER
    ? 'mongodb://mongo:27017'
    : 'mongodb://localhost:27017');

const mongo = new MongoClient(mongoUri);

async function emitirAlerta(db, shipmentId, tipo, severidad, detalle) {
  const alertas = db.collection('alertas');

  const existente = await alertas.findOne({
    shipmentId,
    tipo,
    estado: 'ABIERTA',
  });

  if (existente) return;

  const alerta = {
    eventId: randomUUID(),
    shipmentId,
    tipo,
    severidad,
    detalle,
    estado: 'ABIERTA',
    ocurridoEn: new Date().toISOString(),
    version: 1,
  };

  await alertas.insertOne({
    ...alerta,
    creadaEn: new Date(),
  });

  await producer.send({
    topic: 'alertas.sla',
    messages: [
      {
        key: shipmentId,
        value: JSON.stringify(alerta),
      },
    ],
  });

  console.log(
    `${shipmentId} -> ALERTA SLA -> ${tipo} -> ${severidad}`
  );
}

async function cerrarAlertas(db, shipmentId) {
  await db.collection('alertas').updateMany(
    {
      shipmentId,
      estado: 'ABIERTA',
    },
    {
      $set: {
        estado: 'CERRADA',
        cerradaEn: new Date(),
      },
    }
  );

  console.log(`${shipmentId} -> alertas SLA cerradas`);
}

async function evaluar(db, shipmentId) {
  const slaEnvios = db.collection('sla_envios');

  const envio = await slaEnvios.findOne({
    shipmentId,
  });

  if (!envio || !envio.venceEn) return;

  if (envio.estado === 'ENTREGADO') {
    await cerrarAlertas(db, shipmentId);
    return;
  }

  const ahora = Date.now();
  const vence = new Date(envio.venceEn).getTime();

  const horasRestantes =
    (vence - ahora) / (1000 * 60 * 60);

  if (
    horasRestantes < 0 &&
    envio.estado !== 'ENTREGADO'
  ) {
    await emitirAlerta(
      db,
      shipmentId,
      'VENCIDO',
      'CRITICA',
      'La promesa de entrega se encuentra vencida.'
    );
  }

  if (
    horasRestantes >= 0 &&
    horasRestantes < 4 &&
    envio.estado !== 'EN_REPARTO' &&
    envio.estado !== 'ENTREGADO'
  ) {
    await emitirAlerta(
      db,
      shipmentId,
      'SIN_SALIDA',
      'ALTA',
      'Faltan menos de 4 horas y el envío no está en reparto.'
    );
  }

  if (
    horasRestantes >= 0 &&
    horasRestantes < 10 &&
    ![
      'INGRESADO_HUB',
      'CLASIFICADO',
      'EN_REPARTO',
      'ENTREGADO',
    ].includes(envio.estado)
  ) {
    await emitirAlerta(
      db,
      shipmentId,
      'SIN_INGRESO',
      'MEDIA',
      'Faltan menos de 10 horas y el envío no ingresó al hub.'
    );
  }

  if (
    envio.estado === 'ENTREGA_FALLIDA' &&
    new Date(envio.venceEn).toDateString() ===
      new Date().toDateString()
  ) {
    await emitirAlerta(
      db,
      shipmentId,
      'FALLO_DIA_LIMITE',
      'ALTA',
      'La entrega falló el día límite.'
    );
  }
}

async function main() {
  await mongo.connect();

  const db = mongo.db('rutasur');
  const slaEnvios = db.collection('sla_envios');

  await slaEnvios.createIndex(
    { shipmentId: 1 },
    { unique: true }
  );

  await db.collection('alertas').createIndex({
    shipmentId: 1,
    tipo: 1,
    estado: 1,
  });

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({
    topics: ['envios.estados', 'envios.promesas'],
    fromBeginning: true,
  });

  console.log(
    'SLA Monitor escuchando estados y promesas...'
  );

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const event = JSON.parse(
        message.value.toString()
      );

      if (!event.shipmentId) return;

      if (topic === 'envios.promesas') {
        await slaEnvios.updateOne(
          { shipmentId: event.shipmentId },
          {
            $set: {
              shipmentId: event.shipmentId,
              venceEn: event.venceEn,
              actualizadoEn: new Date(),
            },
          },
          { upsert: true }
        );
      }

      if (topic === 'envios.estados') {
        await slaEnvios.updateOne(
          { shipmentId: event.shipmentId },
          {
            $set: {
              shipmentId: event.shipmentId,
              estado: event.tipo,
              motivo: event.motivo ?? null,
              actualizadoEn: new Date(),
            },
          },
          { upsert: true }
        );
      }

      await evaluar(db, event.shipmentId);
    },
  });

  setInterval(async () => {
    const envios = await slaEnvios
      .find({
        estado: {
          $ne: 'ENTREGADO',
        },
      })
      .toArray();

    for (const envio of envios) {
      await evaluar(db, envio.shipmentId);
    }
  }, 60000);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});