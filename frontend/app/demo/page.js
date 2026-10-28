import Link from 'next/link';

export default function DemoPage() {
  return (
    <main className="container">
      <Link href="/">← Volver</Link>

      <h1>Panel de Demo</h1>

      <p>
        Los escenarios se ejecutan desde el simulador.
      </p>

      <section className="demo">
        <article>
          <h2>Envío feliz</h2>
          <code>node index.js feliz</code>
        </article>

        <article>
          <h2>Entrega fallida - Ausente</h2>
          <code>node index.js ausente</code>
        </article>

        <article>
          <h2>Kafka UI</h2>
          <p>http://localhost:8080</p>
        </article>
      </section>
    </main>
  );
}