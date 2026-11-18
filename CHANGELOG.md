# Registro de cambios

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el versionado es
[semántico](https://semver.org/lang/es/). La versión del artefacto no es la de la API: esta
publica `/api/v1` y seguirá haciéndolo mientras no haya un cambio incompatible de contrato.

## [1.1.0]

### Añadido
- Topología de Kafka Streams que transforma `reservations.events` en `audit.timeline`
  (`TimelineTopology`). La reemisión vivía antes dentro del consumidor, mezclada con la
  persistencia.
- `AuditTimelineConsumer`, que consume `audit.timeline` y persiste. Cierra un ciclo que el
  contrato describía desde el principio y que nadie implementaba: el tema se escribía y no lo
  leía nadie.
- `AuditService`, con la lógica de dominio separada de la mensajería y probable sin broker.
- `KafkaErrorConfig`: reintento con espera creciente de 1 s, 2 s y 4 s, y descarte hacia
  `audit.timeline.DLT` con las cabeceras de origen y de error. Un mensaje imposible de procesar ya
  no bloquea su partición.
- `KafkaConsumerConfig`: consumidor tipado, confirmación manual y concurrencia tres, una por
  partición.
- Primeras pruebas del repositorio: topología con `TopologyTestDriver`, servicio con Mockito y la
  matriz de rutas y autorización del controlador.
- Flujo de integración continua que compila y prueba en cada cambio.

### Corregido
- Las cuatro rutas repetían la versión y respondían en `/api/v1/audit/v1/...`. El frontend y el
  gateway llamaban a las correctas y recibían 404. Ahora la versión se declara una sola vez, en
  `ApiPaths`, y hay una prueba que falla si se vuelve a anteponer.
- `STATUS_BEFORE` nunca se rellenaba, así que el timeline mostraba estados sueltos en vez de
  transiciones. El evento ya trae el estado de origen.
- `OCCURRED_AT` guardaba el momento del consumo. Ahora guarda el del hecho, que es el único que no
  cambia al releer el tema.

### Seguridad
- `spring.json.trusted.packages` pasa de `*` a `cl.andesstay.*`.

## [1.0.0]
- Primera versión: timeline de auditoría y API de consulta.
