import Link from 'next/link';
import { getDb } from '@/lib/mongodb';

export default async function TrackingPage({ params }) {
  const { id } = await params;

  const db = await getDb();

  const envio = await db
    .collection('envios')
    .findOne({ shipmentId: id });

  if (!envio) {
    return (
      <main className="container">
        <Link href="/">← Volver</Link>

        <h1>Tracking</h1>

        <p>Envío no encontrado.</p>
      </main>
    );
  }

  return (
    <main className="container tracking">
      <Link href="/">← Volver</Link>

      <h1>Seguimiento de envío</h1>

      <h2>{envio.shipmentId}</h2>

      <div className="status">
        {envio.estado}
      </div>

      <p>
        Tu envío se encuentra actualmente en estado:
      </p>

      <strong>{envio.estado}</strong>

      <h3>Historial</h3>

      {envio.historial?.map((evento, index) => (
        <p key={index}>
          {evento.tipo} - {evento.ocurridoEn}
        </p>
      ))}
    </main>
  );
}