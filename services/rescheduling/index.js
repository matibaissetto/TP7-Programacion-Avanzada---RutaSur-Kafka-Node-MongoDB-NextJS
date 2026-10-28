const { Kafka } = require('kafkajs');
const { randomUUID } = require('crypto');

const kafka = new Kafka({
  clientId: 'rutasur-rescheduling',
  brokers: ['localhost:29092'],
});

const consumer = kafka.consumer({
  groupId: 'reprogramacion',
});

const producer = kafka.producer();

function calcularReprogramacion(event) {
  const intentos = event.intentos ?? 1;

  if (intentos >= 3) {
    return {
      tipo: 'DEVOLUCION_COMERCIO',
      accion: 'Devolver al comercio',
    };
  }

  switch (event.motivo) {
    case 'AUSENTE':
      return {
        tipo: 'REPROGRAMADO',
        accion: 'Nuevo intento al día hábil siguiente',
        franja: event.franja === 'MANANA' ? 'TARDE' : 'MANANA',
      };

    case 'DIRECCION_INCORRECTA':
      return {
        tipo: 'RETENIDO',
        accion: 'Esperar corrección de dirección',
      };

    case 'RECHAZADO':
      return {
        tipo: 'DEVOLUCION_COMERCIO',
        accion: 'Devolver al comercio',
      };

    case 'ZONA_INACCESIBLE':
      return {
        tipo: 'RETENIDO',
        accion: 'Zona inaccesible',
      };

    default:
      return {
        tipo: 'RETENIDO',
        accion: 'Revisión manual',
      };
  }
}

async function main() {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({
    topic: 'envios.estados',
    fromBeginning: true,
  });

  console.log('Rescheduling escuchando envios.estados...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());

      if (event.tipo !== 'ENTREGA_FALLIDA') {
        return;
      }

      const resultado = calcularReprogramacion(event);

      const reprogramacionEvent = {
        eventId: randomUUID(),
        shipmentId: event.shipmentId,
        tipo: resultado.tipo,
        ocurridoEn: new Date().toISOString(),
        secuencia: event.secuencia,
        version: 1,
        motivo: event.motivo,
        accion: resultado.accion,
        franja: resultado.franja ?? null,
      };

      await producer.send({
        topic: 'envios.reprogramaciones',
        messages: [
          {
            key: event.shipmentId,
            value: JSON.stringify(reprogramacionEvent),
          },
        ],
      });

      console.log(
        `${event.shipmentId} -> ${resultado.tipo} -> ${resultado.accion}`,
      );
    },
  });
}

main().catch(console.error);