const { Kafka } = require('kafkajs');
const { randomUUID } = require('crypto');

const kafka = new Kafka({
  clientId: 'rutasur-notifications',
  brokers: [process.env.KAFKA_BROKER ?? 'localhost:29092'],
});

const consumer = kafka.consumer({
  groupId: 'notificaciones',
});

const producer = kafka.producer();

async function main() {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({
    topic: 'envios.estados',
    fromBeginning: true,
  });

  console.log('Notifications escuchando envios.estados...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());

      if (event.tipo !== 'ENTREGA_FALLIDA') {
        return;
      }

      const notificationEvent = {
        eventId: randomUUID(),
        shipmentId: event.shipmentId,
        tipo: 'NOTIFICACION_ENVIADA',
        ocurridoEn: new Date().toISOString(),
        secuencia: event.secuencia,
        version: 1,
        motivo: event.motivo,
        mensaje: `Entrega fallida por ${event.motivo}. Se informa al destinatario.`,
      };

      await producer.send({
        topic: 'notificaciones.enviadas',
        messages: [
          {
            key: event.shipmentId,
            value: JSON.stringify(notificationEvent),
          },
        ],
      });

      console.log(
        `${event.shipmentId} -> NOTIFICACION_ENVIADA`,
      );
    },
  });
}

main().catch(console.error);
