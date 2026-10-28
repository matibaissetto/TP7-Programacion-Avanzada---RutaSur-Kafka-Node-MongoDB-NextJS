import Link from 'next/link';
import { getDb } from '@/lib/mongodb';

export default async function ShipmentPage({ params }) {
  const { id } = await params;

  const db = await getDb();

  const envio = await db.collection('envios').findOne({
    shipmentId: id,
  });

  if (!envio) {
    return (
      <main className="container">
        <Link href="/">← Volver</Link>
        <h1>Envío no encontrado</h1>
      </main>
    );
  }

  const notificaciones = await db
    .collection('notificaciones')
    .find({ shipmentId: id })
    .sort({ ocurridoEn: 1 })
    .toArray();

  const reprogramaciones = await db
    .collection('reprogramaciones')
    .find({ shipmentId: id })
    .sort({ ocurridoEn: 1 })
    .toArray();

  const alertas = await db
    .collection('alertas')
    .find({ shipmentId: id })
    .sort({ ocurridoEn: 1 })
    .toArray();

  return (
    <main className="container">
      <Link href="/">← Volver</Link>

      <h1>Detalle del envío</h1>

      <section className="cards">
        <article>
          <h3>ID</h3>
          <p>{envio.shipmentId}</p>
        </article>

        <article>
          <h3>Estado</h3>
          <p>{envio.estado}</p>
        </article>

        <article>
          <h3>Hub</h3>
          <p>{envio.hubId ?? '-'}</p>
        </article>

        <article>
          <h3>Zona</h3>
          <p>{envio.zonaId ?? '-'}</p>
        </article>
      </section>

      <h2>Historial</h2>

      {(envio.historial ?? []).map((evento, index) => (
        <p key={index}>
          <strong>{evento.tipo}</strong>
          {' — '}
          {evento.ocurridoEn}
          {evento.motivo
            ? ` — ${evento.motivo}`
            : ''}
        </p>
      ))}

      <h2>Notificaciones</h2>

      {notificaciones.length === 0 ? (
        <p>Sin notificaciones.</p>
      ) : (
        notificaciones.map((item, index) => (
          <p key={index}>
            {item.mensaje ?? item.tipo}
          </p>
        ))
      )}

      <h2>Reprogramaciones</h2>

      {reprogramaciones.length === 0 ? (
        <p>Sin reprogramaciones.</p>
      ) : (
        reprogramaciones.map((item, index) => (
          <p key={index}>
            <strong>
              {item.tipo ??
                item.accion ??
                'REPROGRAMACION'}
            </strong>
            {' — '}
            {item.detalle ??
              item.mensaje ??
              item.nuevaFecha ??
              ''}
          </p>
        ))
      )}

      <h2>Alertas SLA</h2>

      {alertas.length === 0 ? (
        <p>Sin alertas SLA.</p>
      ) : (
        alertas.map((alerta, index) => (
          <p key={index}>
            <strong>{alerta.tipo}</strong>
            {' — '}
            {alerta.severidad}
            {' — '}
            {alerta.estado ?? 'ABIERTA'}
          </p>
        ))
      )}
    </main>
  );
}