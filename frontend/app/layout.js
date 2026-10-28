import './globals.css';
import LiveRefresh from './LiveRefresh';

export const metadata = {
  title: 'RutaSur',
  description: 'Sistema logístico orientado a eventos',
};

export default function RootLayout({ children }) {
  return (
    <html lang="es">
      <body>
        <LiveRefresh />
        {children}
      </body>
    </html>
  );
}