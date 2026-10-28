import Link from 'next/link';
import { getDb } from '@/lib/mongodb';

export default async function Home() {
  const db = await getDb();

  const envios = await db
    .collection('envios')
    .find({})
    .sort({ actualizadoEn: -1 })
    .limit(20)
    .toArray();

  const total = await db.collection('envios').countDocuments();

  const entregados = await db
    .collection('envios')
    .countDocuments({ estado: 'ENTREGADO' });

  const fallidos = await db
    .collection('envios')
    .countDocuments({ estado: 'ENTREGA_FALLIDA' });

  const enReparto = await db
    .collection('envios')
    .countDocuments({ estado: 'EN_REPARTO' });

  return (
    <main className="container">
      <h1>RutaSur - Torre de Control</h1>

      <nav>
        <Link href="/">Torre de control</Link>
        <Link href="/analytics">Analítica</Link>
        <Link href="/demo">Panel Demo</Link>
      </nav>

      <section className="cards">
        <article>
          <h2>{total}</h2>
          <p>Total envíos</p>
        </article>

        <article>
          <h2>{enReparto}</h2>
          <p>En reparto</p>
        </article>

        <article>
          <h2>{entregados}</h2>
          <p>Entregados</p>
        </article>

        <article>
          <h2>{fallidos}</h2>
          <p>Fallidos</p>
        </article>
      </section>

      <section>
        <h2>Últimos envíos</h2>

        <table>
          <thead>
            <tr>
              <th>Shipment ID</th>
              <th>Estado</th>
              <th>Zona</th>
              <th>Hub</th>
              <th>Detalle</th>
              <th>Tracking</th>
            </tr>
          </thead>

          <tbody>
            {envios.map((envio) => (
              <tr key={envio.shipmentId}>
                <td>{envio.shipmentId}</td>
                <td>{envio.estado}</td>
                <td>{envio.zonaId ?? '-'}</td>
                <td>{envio.hubId ?? '-'}</td>
                <td>
                  <Link href={`/shipment/${envio.shipmentId}`}>
                    Ver
                  </Link>
                </td>
                <td>
                  <Link href={`/tracking/${envio.shipmentId}`}>
                    Público
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}