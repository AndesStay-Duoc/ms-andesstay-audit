package cl.andesstay.audit.streams;

import cl.andesstay.audit.messaging.events.Actor;
import cl.andesstay.audit.messaging.events.ReservationEvent;
import cl.andesstay.audit.messaging.events.TimelineEvent;
import cl.andesstay.audit.messaging.events.TimelinePayload;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El JSON que la topologia escribe en audit.timeline, y lo que acepta leer de reservations.events.
 *
 * POR QUE EXISTE. El contrato fija timestamp en ISO 8601. El JsonSerde con su ObjectMapper por
 * defecto escribia 1795255200.000000000. Nada fallaba, porque el consumidor de audit lo lee igual,
 * pero audit.timeline no publicaba lo que el contrato promete.
 *
 * LA COMPATIBILIDAD. En reservations.events ya hay mensajes con la fecha numerica, escritos antes de
 * corregir el productor. El Serde de entrada tiene que seguir leyendolos, o una relectura del tema
 * desde el principio los mandaria todos a la basura.
 */
class SerdesDeAuditoriaFormatoTest {

    private static final ObjectMapper LECTOR = new ObjectMapper();
    private static final Instant MOMENTO = Instant.parse("2026-11-21T10:00:00Z");

    private final SerdesDeAuditoria serdes = new SerdesDeAuditoria();

    @Test
    @DisplayName("audit.timeline: timestamp sale en ISO 8601, no como segundos desde 1970")
    void timelineEnIso() throws Exception {
        TimelineEvent hecho = new TimelineEvent("reservation.created", "evt-1", MOMENTO,
                "traza-1", "corr-1", "ms-andesstay-audit", new Actor("u-1", "Guest", "andesstay"),
                new TimelinePayload(TimelinePayload.ENTIDAD_RESERVA, "42",
                        TimelinePayload.ACCION_CREACION, null, "CREADA", 7L, "g-1"));

        byte[] escrito = serdes.timelineEvent().serializer().serialize("audit.timeline", hecho);
        JsonNode json = LECTOR.readTree(escrito);

        assertThat(json.path("timestamp").isTextual()).isTrue();
        assertThat(json.path("timestamp").asText()).isEqualTo("2026-11-21T10:00:00Z");
    }

    @Test
    @DisplayName("reservations.events: lee tanto ISO 8601 como la fecha numerica antigua")
    void leeLosDosFormatos() {
        String iso = "{\"type\":\"reservation.created\",\"eventId\":\"e-1\","
                + "\"timestamp\":\"2026-11-21T10:00:00Z\",\"payload\":{\"reservationId\":42}}";
        String numerico = "{\"type\":\"reservation.created\",\"eventId\":\"e-2\","
                + "\"timestamp\":1795255200.000000000,\"payload\":{\"reservationId\":42}}";

        var lector = serdes.reservationEvent().deserializer();
        ReservationEvent deIso = lector.deserialize("reservations.events", iso.getBytes(StandardCharsets.UTF_8));
        ReservationEvent deNumero = lector.deserialize("reservations.events", numerico.getBytes(StandardCharsets.UTF_8));

        assertThat(deIso.timestamp()).isEqualTo(MOMENTO);
        assertThat(deNumero.timestamp()).isEqualTo(MOMENTO);
    }
}
