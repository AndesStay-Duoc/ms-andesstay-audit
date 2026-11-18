package cl.andesstay.audit.messaging.events;

import java.time.Instant;

/**
 * El envelope de los eventos que llegan por reservations.events.
 *
 * Copia del record que publica ms-andesstay-reservations; ver el comentario de Actor. Hasta ahora
 * este servicio consumia un Map&lt;String, Object&gt; y leia cada campo con un cast: un nombre mal
 * escrito se convertia en un null silencioso y la columna correspondiente quedaba vacia sin que
 * nadie se enterara. Con un record, el formato equivocado falla al deserializar y el mensaje va a
 * la cola de descarte, que es el comportamiento que se quiere.
 */
public record ReservationEvent(
        String type,
        String eventId,
        Instant timestamp,
        String traceId,
        String correlationId,
        String source,
        Actor actor,
        ReservationPayload payload) {
}
