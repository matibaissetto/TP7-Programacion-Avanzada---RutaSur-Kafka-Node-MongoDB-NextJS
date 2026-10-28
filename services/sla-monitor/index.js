const { Kafka } = require('kafkajs');
const { randomUUID } = require('crypto');

const kafka = new Kafka({
  clientId: 'rutasur-sla-monitor',
  brokers: ['localhost:29092'],
});

const consumer = kafka.consumer({
  groupId: 'monitor-sla',
});

const producer = kafka.producer();

const envios = new Map();
const alertasEmitidas = new Set();

function obtenerEstado(shipmentId) {
  if (!envios.has(shipmentId)) {
    envios.set(shipmentId, {
      shipmentId,
      estado: null,
      venceEn: null,
    });
  }

  return envios.get(shipmentId);
}

async function emitirAlerta(envio, regla, prioridad) {
  const clave = `${envio.shipmentId}-${regla}`;

  if (alertasEmitidas.has(clave)) {
    return;
  }

  alertasEmitidas.add(clave);

  const alerta = {
    eventId: randomUUID(),
    shipmentId: envio.shipmentId,
    tipo: 'ALERTA_SLA',
    regla,
    prioridad,
    ocurridoEn: new Date().toISOString(),
    version: 1,
  };

  await producer.send({
    topic: 'alertas.sla',
    messages: [
      {
        key: envio.shipmentId,
        value: JSON.stringify(alerta),
      },
    ],
  });

  console.log(
    `${envio.shipmentId} -> ALERTA SLA -> ${regla} -> ${prioridad}`,
  );
}

async function evaluar(envio) {
  if (!envio.venceEn || envio.estado === 'ENTREGADO') {
    return;
  }

  const ahora = Date.now();
  const vence = new Date(envio.venceEn).getTime();
  const horas = (vence - ahora) / 3600000;

  if (horas < 0) {
    await emitirAlerta(envio, 'VENCIDO', 'CRITICA');
    return;
  }

  if (
    horas < 4 &&
    envio.estado !== 'EN_REPARTO'
  ) {
    await emitirAlerta(envio, 'SIN_SALIDA', 'ALTA');
  }

  if (
    horas < 10 &&
    !['INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO'].includes(envio.estado)
  ) {
    await emitirAlerta(envio, 'SIN_INGRESO', 'MEDIA');
  }

  if (
    envio.estado === 'ENTREGA_FALLIDA' &&
    new Date(envio.venceEn).toDateString() === new Date().toDateString()
  ) {
    await emitirAlerta(envio, 'FALLO_DIA_LIMITE', 'ALTA');
  }
}

async function main() {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({
    topics: ['envios.estados', 'envios.promesas'],
    fromBeginning: true,
  });

  console.log('SLA Monitor escuchando estados y promesas...');

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const event = JSON.parse(message.value.toString());

      const envio = obtenerEstado(event.shipmentId);

      if (topic === 'envios.promesas') {
        envio.venceEn = event.venceEn;
      }

      if (topic === 'envios.estados') {
        envio.estado = event.tipo;
      }

      await evaluar(envio);
    },
  });

  setInterval(async () => {
    for (const envio of envios.values()) {
      await evaluar(envio);
    }
  }, 60000);
}

main().catch(console.error);