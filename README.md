# ms-andesstay-audit

Microservicio de **auditoría** del sistema AndesStay. Consume el tópico Kafka `audit.timeline`,
persiste los eventos y expone el timeline de una reserva en modo **solo lectura**.

Responde la pregunta "quién hizo qué, cuándo y desde dónde" sobre cada reserva de hospedaje.

## Responsabilidad

- Consumir `audit.timeline` desde Kafka.
- Persistir cada evento en el esquema Oracle `AUDIT`.
- Exponer `/api/audit/*` con filtros por usuario, rango de fechas y tipo de evento.

No expone escritura: el timeline se construye solo a partir de los eventos.

## Endpoints

| Método | Ruta | Rol |
|---|---|---|
| `GET` | `/api/audit?reservationId=&userId=&from=&to=&type=` | Admin, Auditor |
| `GET` | `/api/audit/{reservationId}` | Admin, Auditor |

Se accede siempre a través del BFF, detrás del API Gateway. Ver
[`rutas-gateway.md`](https://github.com/AndesStay-Duoc/infra/blob/develop/docs/contracts/rutas-gateway.md).

## Stack

Java 21 · Spring Boot 3.5 · Spring Kafka · Spring Data JPA · Oracle · Spring Security como
resource server.

## Variables de entorno

| Variable | Descripción |
|---|---|
| `DB_URL` | JDBC del esquema `AUDIT`, por ejemplo `jdbc:oracle:thin:@//host:1521/FREEPDB1` |
| `DB_USER` | Usuario del esquema `AUDIT` |
| `DB_PASSWORD` | Contraseña del esquema |
| `KAFKA_BOOTSTRAP_SERVERS` | Lista de brokers |
| `JWT_ISSUER_STAFF` | Issuer del tenant corporativo |
| `JWT_AUDIENCE_STAFF` | Audience esperada del token corporativo |

Se configuran en un archivo `.env` que **no se versiona**. Ver `.env.example`.

## Cómo levantarlo

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev
```

Requiere Oracle y Kafka en marcha. Los compose están en el repositorio
[`infra`](https://github.com/AndesStay-Duoc/infra).

## Contratos

Los esquemas de eventos y el envelope común son canónicos y viven en
[`infra/docs/contracts/`](https://github.com/AndesStay-Duoc/infra/tree/develop/docs/contracts).
Todo consumidor descarta duplicados por `eventId`.

## Cómo contribuir

Ver [`CONTRIBUTING.md`](CONTRIBUTING.md).
