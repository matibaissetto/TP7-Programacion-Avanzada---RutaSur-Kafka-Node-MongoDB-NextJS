import Link from 'next/link';
import { getDb } from '@/lib/mongodb';

export default async function ShipmentPage({ params }) {
  const { id } = await params;

  const db = await getDb();

  const envio = await db
    .collection('envios')
    .findOne({ shipmentId: id });

  if (!envio) {
    return (
      <main className="container">
        <h1>Envío no encontrado</h1>
        <Link href="/">Volver</Link>
      </main>
    );
  }

  return (
    <main className="container">
      <Link href="/">← Volver</Link>

      <h1>Detalle del envío</h1>

      <section className="detail">
        <p>
          <strong>ID:</strong> {envio.shipmentId}
        </p>

        <p>
          <strong>Estado:</strong> {envio.estado}
        </p>

        <p>
          <strong>Hub:</strong> {envio.hubId ?? '-'}
        </p>

        <p>
          <strong>Zona:</strong> {envio.zonaId ?? '-'}
        </p>

        <p>
          <strong>Repartidor:</strong>{' '}
          {envio.repartidorId ?? '-'}
        </p>
      </section>

      <h2>Historial</h2>

      <div className="timeline">
        {envio.historial?.map((evento, index) => (
          <article key={index}>
            <strong>{evento.tipo}</strong>
            <p>{evento.ocurridoEn}</p>
            <small>Secuencia {evento.secuencia}</small>
          </article>
        ))}
      </div>
    </main>
  );
}