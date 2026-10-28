const { Kafka } = require('kafkajs');
const { randomUUID } = require('crypto');

const kafka = new Kafka({
  clientId: 'rutasur-simulator',
  brokers: ['localhost:29092'],
});

const producer = kafka.producer();

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

function evento(shipmentId, tipo, secuencia, extra = {}) {
  return {
    eventId: randomUUID(),
    shipmentId,
    tipo,
    ocurridoEn: new Date().toISOString(),
    secuencia,
    hubId: 'HUB-1',
    zonaId: 'ZONA-1',
    repartidorId: 'REP-1',
    version: 1,
    ...extra,
  };
}

async function publicar(topic, data) {
  await producer.send({
    topic,
    messages: [
      {
        key: data.shipmentId ?? 'invalid',
        value: JSON.stringify(data),
      },
    ],
  });

  console.log(
    `${topic} -> ${data.tipo ?? 'INVALIDO'} -> ${data.shipmentId ?? '-'}`,
  );
}

async function publicarPromesa(shipmentId, horas = 24) {
  await publicar('envios.promesas', {
    eventId: randomUUID(),
    shipmentId,
    tipo: 'PROMESA_CREADA',
    ocurridoEn: new Date().toISOString(),
    secuencia: 0,
    venceEn: new Date(
      Date.now() + horas * 3600000,
    ).toISOString(),
    version: 1,
  });
}

async function envioFeliz() {
  const shipmentId = `ENV-${Date.now()}`;

  await publicarPromesa(shipmentId);

  const estados = [
    'CREADO',
    'INGRESADO_HUB',
    'CLASIFICADO',
    'EN_REPARTO',
    'ENTREGADO',
  ];

  for (let i = 0; i < estados.length; i++) {
    await publicar(
      'envios.estados',
      evento(shipmentId, estados[i], i + 1),
    );

    await sleep(300);
  }

  console.log(`Envío feliz terminado: ${shipmentId}`);
}

async function envioAusente() {
  const shipmentId = `ENV-${Date.now()}`;

  await publicarPromesa(shipmentId);

  const estados = [
    'CREADO',
    'INGRESADO_HUB',
    'CLASIFICADO',
    'EN_REPARTO',
  ];

  for (let i = 0; i < estados.length; i++) {
    await publicar(
      'envios.estados',
      evento(shipmentId, estados[i], i + 1),
    );

    await sleep(300);
  }

  await publicar(
    'envios.estados',
    evento(shipmentId, 'ENTREGA_FALLIDA', 5, {
      motivo: 'AUSENTE',
      intentos: 1,
      franja: 'MANANA',
    }),
  );

  console.log(`Escenario AUSENTE terminado: ${shipmentId}`);
}

async function tercerFallo() {
  const shipmentId = `ENV-${Date.now()}`;

  await publicarPromesa(shipmentId);

  await publicar(
    'envios.estados',
    evento(shipmentId, 'CREADO', 1),
  );

  await publicar(
    'envios.estados',
    evento(shipmentId, 'EN_REPARTO', 2),
  );

  await publicar(
    'envios.estados',
    evento(shipmentId, 'ENTREGA_FALLIDA', 3, {
      motivo: 'AUSENTE',
      intentos: 3,
      franja: 'TARDE',
    }),
  );

  console.log(`Escenario TERCER FALLO terminado: ${shipmentId}`);
}

async function demorado() {
  const shipmentId = `ENV-${Date.now()}`;

  await publicarPromesa(shipmentId, 3);

  await publicar(
    'envios.estados',
    evento(shipmentId, 'CREADO', 1),
  );

  await publicar(
    'envios.estados',
    evento(shipmentId, 'INGRESADO_HUB', 2),
  );

  console.log(`Escenario DEMORADO creado: ${shipmentId}`);
}

async function invalido() {
  await publicar('envios.estados', {
    eventId: randomUUID(),
    tipo: 'CREADO',
  });

  console.log('Evento inválido enviado');
}

async function fueraDeOrden() {
  const shipmentId = `ENV-${Date.now()}`;

  await publicar(
    'envios.estados',
    evento(shipmentId, 'CREADO', 1),
  );

  await publicar(
    'envios.estados',
    evento(shipmentId, 'EN_REPARTO', 3),
  );

  await publicar(
    'envios.estados',
    evento(shipmentId, 'CLASIFICADO', 2),
  );

  console.log(`Evento fuera de orden enviado: ${shipmentId}`);
}

async function main() {
  await producer.connect();

  const escenario = process.argv[2] ?? 'feliz';

  if (escenario === 'feliz') {
    await envioFeliz();
  } else if (escenario === 'ausente') {
    await envioAusente();
  } else if (escenario === 'tercer-fallo') {
    await tercerFallo();
  } else if (escenario === 'demorado') {
    await demorado();
  } else if (escenario === 'invalido') {
    await invalido();
  } else if (escenario === 'fuera-orden') {
    await fueraDeOrden();
  } else {
    console.log('Escenario no reconocido');
  }

  await producer.disconnect();
}

main().catch(console.error);