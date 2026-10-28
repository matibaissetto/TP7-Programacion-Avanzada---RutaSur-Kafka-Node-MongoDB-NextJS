const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const kafka = new Kafka({
  clientId: 'rutasur-analytics',
  brokers: [process.env.KAFKA_BROKER ?? 'localhost:29092'],
});

const consumer = kafka.consumer({
  groupId: 'analitica-v2',
});

const mongo = new MongoClient(
  process.env.MONGO_URI ?? 'mongodb://localhost:27017'
);

function fechaDia(iso) {
  return new Date(iso ?? Date.now())
    .toISOString()
    .slice(0, 10);
}

function franjaHora(iso) {
  const hora = new Date(iso ?? Date.now()).getHours();

  if (hora < 12) return 'MANANA';
  if (hora < 18) return 'TARDE';

  return 'NOCHE';
}

async function recalcularMetricas(db) {
  const envios = db.collection('envios');
  const metricas = db.collection('metricas_resumen');

  const todos = await envios.find({}).toArray();

  let totalConIntento = 0;
  let entregadosPrimerIntento = 0;
  let totalEntregados = 0;
  let totalConPromesa = 0;
  let dentroSla = 0;

  const motivos = {};

  for (const envio of todos) {
    const historial = envio.historial ?? [];

    const fallos = historial.filter(
      (e) => e.tipo === 'ENTREGA_FALLIDA'
    );

    const entregado = historial.find(
      (e) => e.tipo === 'ENTREGADO'
    );

    if (fallos.length > 0 || entregado) {
      totalConIntento++;
    }

    if (entregado) {
      totalEntregados++;

      if (fallos.length === 0) {
        entregadosPrimerIntento++;
      }
    }

    for (const fallo of fallos) {
      const motivo = fallo.motivo ?? 'SIN_MOTIVO';

      motivos[motivo] =
        (motivos[motivo] ?? 0) + 1;
    }

    const sla = await db
      .collection('sla_envios')
      .findOne({
        shipmentId: envio.shipmentId,
      });

    if (sla?.venceEn && entregado?.ocurridoEn) {
      totalConPromesa++;

      if (
        new Date(entregado.ocurridoEn) <=
        new Date(sla.venceEn)
      ) {
        dentroSla++;
      }
    }
  }

  const tasaPrimerIntento =
    totalConIntento > 0
      ? (entregadosPrimerIntento /
          totalConIntento) *
        100
      : 0;

  const cumplimientoSla =
    totalConPromesa > 0
      ? (dentroSla / totalConPromesa) * 100
      : 0;

  await metricas.updateOne(
    { clave: 'general' },
    {
      $set: {
        clave: 'general',
        totalEnvios: todos.length,
        totalEntregados,
        totalConIntento,
        entregadosPrimerIntento,
        tasaEntregaPrimerIntento:
          Number(tasaPrimerIntento.toFixed(2)),
        distribucionFallos: motivos,
        totalConPromesa,
        dentroSla,
        cumplimientoSla:
          Number(cumplimientoSla.toFixed(2)),
        actualizadoEn: new Date(),
      },
    },
    { upsert: true }
  );
}

async function procesarEvento(db, event) {
  const metricasDiarias =
    db.collection('metricas_diarias');

  const fecha = fechaDia(event.ocurridoEn);

  await metricasDiarias.updateOne(
    {
      fecha,
      zonaId: event.zonaId ?? 'SIN_ZONA',
      hubId: event.hubId ?? 'SIN_HUB',
      franja: event.franja ??
        franjaHora(event.ocurridoEn),
      motivo: event.motivo ?? 'SIN_MOTIVO',
    },
    {
      $setOnInsert: {
        fecha,
        zonaId: event.zonaId ?? 'SIN_ZONA',
        hubId: event.hubId ?? 'SIN_HUB',
        franja:
          event.franja ??
          franjaHora(event.ocurridoEn),
        motivo: event.motivo ?? 'SIN_MOTIVO',
      },
      $inc: {
        totalEventos: 1,
        entregados:
          event.tipo === 'ENTREGADO' ? 1 : 0,
        fallos:
          event.tipo === 'ENTREGA_FALLIDA'
            ? 1
            : 0,
      },
    },
    { upsert: true }
  );

  await recalcularMetricas(db);
}

async function replayTiempoHub(db) {
  const envios = await db
    .collection('envios')
    .find({})
    .toArray();

  const porHub = {};

  for (const envio of envios) {
    const historial = envio.historial ?? [];

    const ingreso = historial.find(
      (e) => e.tipo === 'INGRESADO_HUB'
    );

    const reparto = historial.find(
      (e) => e.tipo === 'EN_REPARTO'
    );

    if (!ingreso || !reparto) continue;

    const ms =
      new Date(reparto.ocurridoEn) -
      new Date(ingreso.ocurridoEn);

    const hub = envio.hubId ?? 'SIN_HUB';

    if (!porHub[hub]) {
      porHub[hub] = {
        sumaMs: 0,
        cantidad: 0,
      };
    }

    porHub[hub].sumaMs += ms;
    porHub[hub].cantidad++;
  }

  const resultados = Object.entries(
    porHub
  ).map(([hubId, data]) => ({
    hubId,
    promedioMinutos:
      data.cantidad > 0
        ? Number(
            (
              data.sumaMs /
              data.cantidad /
              60000
            ).toFixed(2)
          )
        : 0,
    cantidad: data.cantidad,
  }));

  await db
    .collection('metricas_replay')
    .deleteMany({});

  if (resultados.length > 0) {
    await db
      .collection('metricas_replay')
      .insertMany(resultados);
  }

  console.log(
    'Replay analítico recalculado'
  );
}

async function main() {
  await mongo.connect();

  const db = mongo.db('rutasur');

  await consumer.connect();

  await consumer.subscribe({
    topic: 'envios.estados',
    fromBeginning: true,
  });

  console.log(
    'Analytics escuchando envios.estados...'
  );

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(
        message.value.toString()
      );

      await procesarEvento(db, event);

      console.log(
        `${event.shipmentId ?? '-'} -> ANALYTICS -> ${event.tipo}`
      );
    },
  });

  setInterval(async () => {
    await replayTiempoHub(db);
  }, 60000);

  await replayTiempoHub(db);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});