import Link from 'next/link';
import { getDb } from '@/lib/mongodb';

export default async function AnalyticsPage() {
  const db = await getDb();

  const metricas = await db
    .collection('metricas_diarias')
    .find({})
    .sort({ fecha: -1 })
    .toArray();

  const totales = metricas.reduce(
    (acc, item) => {
      acc.entregados += item.entregados ?? 0;
      acc.fallos += item.fallos ?? 0;
      return acc;
    },
    {
      entregados: 0,
      fallos: 0,
    },
  );

  const tasa =
    totales.entregados + totales.fallos > 0
      ? (
          (totales.entregados /
            (totales.entregados + totales.fallos)) *
          100
        ).toFixed(2)
      : 0;

  return (
    <main className="container">
      <Link href="/">← Volver</Link>

      <h1>Analítica RutaSur</h1>

      <section className="cards">
        <article>
          <h2>{totales.entregados}</h2>
          <p>Entregados</p>
        </article>

        <article>
          <h2>{totales.fallos}</h2>
          <p>Fallos</p>
        </article>

        <article>
          <h2>{tasa}%</h2>
          <p>Tasa de entrega</p>
        </article>
      </section>

      <h2>Métricas por zona y franja</h2>

      <table>
        <thead>
          <tr>
            <th>Fecha</th>
            <th>Zona</th>
            <th>Hub</th>
            <th>Franja</th>
            <th>Motivo</th>
            <th>Entregados</th>
            <th>Fallos</th>
          </tr>
        </thead>

        <tbody>
          {metricas.map((metrica, index) => (
            <tr key={index}>
              <td>{metrica.fecha}</td>
              <td>{metrica.zonaId}</td>
              <td>{metrica.hubId}</td>
              <td>{metrica.franja}</td>
              <td>{metrica.motivo}</td>
              <td>{metrica.entregados ?? 0}</td>
              <td>{metrica.fallos ?? 0}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  );
}