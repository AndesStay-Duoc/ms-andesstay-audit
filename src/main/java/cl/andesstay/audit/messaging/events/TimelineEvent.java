package cl.andesstay.audit.messaging.events;

import java.time.Instant;

/**
 * El hecho tal como queda registrado en audit.timeline: quien, que, cuando y entre que estados.
 *
 * Conserva el eventId, el traceId y el correlationId del evento de origen. Eso es lo que permite
 * tres cosas distintas: descartar duplicados por eventId, seguir una peticion concreta por traceId
 * y reconstruir la vida entera de una reserva por correlationId.
 */
public record TimelineEvent(
        String type,
        String eventId,
        Instant timestamp,
        String traceId,
        String correlationId,
        String source,
        Actor actor,
        TimelinePayload payload) {
}
