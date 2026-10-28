# TP7 - Programación Avanzada

## RutaSur - Logística orientada a eventos

Sistema distribuido para seguimiento de envíos basado en arquitectura orientada a eventos.

## Tecnologías

- Apache Kafka
- Node.js
- KafkaJS
- MongoDB
- Next.js
- Docker
- Docker Compose
- Kafka UI

## Componentes

- Simulator
- Projector
- Notifications
- Rescheduling
- SLA Monitor
- Analytics
- Frontend Next.js

## Topics Kafka

- `envios.estados`
- `envios.promesas`
- `envios.reprogramaciones`
- `alertas.sla`
- `notificaciones.enviadas`
- `envios.estados.dlq`

## Funcionalidades implementadas

### Ciclo de vida

Estados principales:

- CREADO
- INGRESADO_HUB
- CLASIFICADO
- EN_REPARTO
- ENTREGADO
- ENTREGA_FALLIDA

El projector consume los eventos y mantiene el estado actual y el historial de cada envío en MongoDB.

### Entrega fallida

Un evento `ENTREGA_FALLIDA` es procesado de forma independiente por:

- Notifications
- Rescheduling
- Analytics

Para el motivo `AUSENTE` se genera una notificación y una reprogramación.

El tercer intento fallido genera devolución al comercio.

### SLA

El monitor consume estados y promesas y contempla alertas:

- SIN_INGRESO
- SIN_SALIDA
- FALLO_DIA_LIMITE
- VENCIDO

### Idempotencia y orden

El projector registra los `eventId` procesados.

Los eventos duplicados son descartados.

Los eventos cuya secuencia es menor o igual a la última aplicada no modifican el estado del envío.

### DLQ

Los eventos inválidos son publicados en:

`envios.estados.dlq`

## Escenarios del simulador

Desde la carpeta `simulator`:

```bash
node index.js feliz
```

```bash
node index.js ausente
```

```bash
node index.js tercer-fallo
```

```bash
node index.js demorado
```

```bash
node index.js invalido
```

```bash
node index.js fuera-orden
```

## Frontend

La interfaz Next.js incluye:

- Torre de control
- Detalle del envío
- Tracking público
- Analítica
- Panel de demo

Frontend:

```text
http://localhost:3000
```

Kafka UI:

```text
http://localhost:8080
```

## Infraestructura

Levantar Kafka, MongoDB y Kafka UI:

```bash
docker compose up -d
```

## Ejecución de servicios

Cada servicio Node.js puede iniciarse con:

```bash
node index.js
```

El frontend se inicia desde `frontend` con:

```bash
npm run dev
```