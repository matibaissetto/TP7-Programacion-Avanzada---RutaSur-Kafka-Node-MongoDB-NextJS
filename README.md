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
- Server-Sent Events

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

### Ciclo de vida de envíos

Estados:

- CREADO
- INGRESADO_HUB
- CLASIFICADO
- EN_REPARTO
- ENTREGADO
- ENTREGA_FALLIDA

El Projector mantiene en MongoDB el estado actual y el historial de cada envío.

### Idempotencia y orden

El sistema registra los `eventId` procesados.

Los eventos duplicados se descartan y los eventos cuya secuencia es menor o igual a la última aplicada no modifican el estado.

### DLQ

Los eventos inválidos se publican en:

`envios.estados.dlq`

### Entrega fallida

Un evento `ENTREGA_FALLIDA` es procesado independientemente por:

- Notifications
- Rescheduling
- Analytics

Se contemplan los motivos:

- AUSENTE
- DIRECCION_INCORRECTA
- RECHAZADO
- ZONA_INACCESIBLE

El tercer intento fallido genera devolución al comercio.

### SLA

El monitor SLA evalúa eventos y periódicamente controla:

- SIN_INGRESO
- SIN_SALIDA
- FALLO_DIA_LIMITE
- VENCIDO

Las alertas se persisten en MongoDB, no se repiten mientras estén abiertas y se cierran cuando el envío se entrega.

### Analytics

Se calculan:

- Tasa de entrega en primer intento
- Distribución de motivos de fallo
- Cumplimiento de SLA

También se realiza un replay para calcular el tiempo promedio entre `INGRESADO_HUB` y `EN_REPARTO` por hub.

### Frontend

La aplicación incluye:

- Torre de control
- Detalle completo del envío
- Tracking público
- Analítica
- Panel demo

El detalle muestra:

- Historial
- Notificaciones
- Reprogramaciones
- Alertas SLA

La interfaz utiliza SSE y MongoDB Change Streams para actualizarse automáticamente.

## Escenarios del simulador

- `feliz`
- `ausente`
- `tercer-fallo`
- `demorado`
- `invalido`
- `fuera-orden`
- `burst`

Ejemplo:

```bash
docker compose run --rm simulator node index.js burst
```

## Ejecución

Todo el entorno se inicia con:

```bash
docker compose up -d --build
```

Frontend:

```text
http://localhost:3000
```

Kafka UI:

```text
http://localhost:8080
```

Para detener todo:

```bash
docker compose down
```