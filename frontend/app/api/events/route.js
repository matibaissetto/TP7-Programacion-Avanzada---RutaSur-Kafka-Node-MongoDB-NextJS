import { getDb } from '@/lib/mongodb';

export const runtime = 'nodejs';

export async function GET(request) {
  const db = await getDb();
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      const changeStream = db.watch([], {
        fullDocument: 'updateLookup',
      });

      const enviar = (change) => {
        try {
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({
                operationType: change.operationType,
                collection:
                  change.ns?.coll ?? null,
              })}\n\n`
            )
          );
        } catch {}
      };

      changeStream.on('change', enviar);

      const heartbeat = setInterval(() => {
        try {
          controller.enqueue(
            encoder.encode(': heartbeat\n\n')
          );
        } catch {}
      }, 15000);

      request.signal.addEventListener('abort', async () => {
        clearInterval(heartbeat);

        try {
          await changeStream.close();
        } catch {}

        try {
          controller.close();
        } catch {}
      });
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
    },
  });
}