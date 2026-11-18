package cl.andesstay.audit.streams;

import cl.andesstay.audit.messaging.events.Actor;
import cl.andesstay.audit.messaging.events.ReservationEvent;
import cl.andesstay.audit.messaging.events.ReservationPayload;
import cl.andesstay.audit.messaging.events.TimelineEvent;
import cl.andesstay.audit.messaging.events.TimelinePayload;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * La topologia de Streams, probada sin broker.
 *
 * TopologyTestDriver ejecuta la topologia real —los mismos Serde, el mismo filtro, el mismo
 * mapeo— contra temas en memoria. Es la forma de comprobar la transformacion en milisegundos y
 * sin Docker, y es lo que permite que estas reglas se verifiquen en cada compilacion.
 */
class TimelineTopologyTest {

    private static final String TEMA_ENTRADA = "reservations.events";
    private static final String TEMA_SALIDA = "audit.timeline";

    private TopologyTestDriver driver;
    private TestInputTopic<String, ReservationEvent> entrada;
    private TestOutputTopic<String, TimelineEvent> salida;

    @BeforeEach
    void montarLaTopologia() {
        SerdesDeAuditoria serdes = new SerdesDeAuditoria();
        TimelineTopology topologia = new TimelineTopology();
        ReflectionTestUtils.setField(topologia, "temaDeEventos", TEMA_ENTRADA);
        ReflectionTestUtils.setField(topologia, "temaDeTimeline", TEMA_SALIDA);

        StreamsBuilder builder = new StreamsBuilder();
        topologia.timelineStream(builder, serdes);

        Properties propiedades = new Properties();
        propiedades.put(StreamsConfig.APPLICATION_ID_CONFIG, "prueba-timeline");
        propiedades.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "no-se-usa:9092");

        driver = new TopologyTestDriver(builder.build(), propiedades);
        entrada = driver.createInputTopic(TEMA_ENTRADA,
                Serdes.String().serializer(), serdes.reservationEvent().serializer());
        salida = driver.createOutputTopic(TEMA_SALIDA,
                Serdes.String().deserializer(), serdes.timelineEvent().deserializer());
    }

    @AfterEach
    void cerrar() {
        driver.close();
    }

    @Test
    @DisplayName("La creacion se registra como CREACION, sin estado de origen")
    void creacion() {
        entrada.pipeInput("42", evento("reservation.created", null, "CREADA"));

        TimelineEvent hecho = salida.readValue();
        assertThat(hecho.type()).isEqualTo("reservation.created");
        assertThat(hecho.payload().accion()).isEqualTo(TimelinePayload.ACCION_CREACION);
        assertThat(hecho.payload().entidad()).isEqualTo(TimelinePayload.ENTIDAD_RESERVA);
        assertThat(hecho.payload().entidadId()).isEqualTo("42");
        assertThat(hecho.payload().desde()).isNull();
        assertThat(hecho.payload().hacia()).isEqualTo("CREADA");
    }

    @Test
    @DisplayName("Una transicion se registra como CAMBIO_ESTADO con origen y destino")
    void transicion() {
        entrada.pipeInput("42", evento("reservation.confirmed", "CREADA", "CONFIRMADA"));

        TimelineEvent hecho = salida.readValue();
        assertThat(hecho.payload().accion()).isEqualTo(TimelinePayload.ACCION_CAMBIO_ESTADO);
        assertThat(hecho.payload().desde()).isEqualTo("CREADA");
        assertThat(hecho.payload().hacia()).isEqualTo("CONFIRMADA");
    }

    @Test
    @DisplayName("El sobre se conserva: eventId, traceId, correlationId y actor")
    void conservaElSobre() {
        entrada.pipeInput("42", evento("reservation.confirmed", "CREADA", "CONFIRMADA"));

        TimelineEvent hecho = salida.readValue();
        assertThat(hecho.eventId()).isEqualTo("evt-1");
        assertThat(hecho.traceId()).isEqualTo("traza-1");
        assertThat(hecho.correlationId()).isEqualTo("corr-1");
        assertThat(hecho.source()).isEqualTo("ms-andesstay-reservations");
        assertThat(hecho.actor().userId()).isEqualTo("sub-operador");
        assertThat(hecho.actor().role()).isEqualTo("Operador");
    }

    @Test
    @DisplayName("La clave no cambia, para que los hechos de una reserva sigan en orden")
    void conservaLaClave() {
        entrada.pipeInput("42", evento("reservation.confirmed", "CREADA", "CONFIRMADA"));

        assertThat(salida.readKeyValue().key).isEqualTo("42");
    }

    @Test
    @DisplayName("Un evento sin reserva se descarta y no llega al timeline")
    void descartaLoQueNoIdentificaEntidad() {
        ReservationEvent sinReserva = new ReservationEvent(
                "reservation.created", "evt-2", Instant.now(), "traza", "corr",
                "ms-andesstay-reservations", new Actor("sub", "Operador", "staff"),
                new ReservationPayload(null, null, null, null, null, null, null,
                        "CREADA", null, null, null, null, null));

        entrada.pipeInput("sin-id", sinReserva);

        assertThat(salida.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Un evento sin eventId se descarta: sin el no hay idempotencia posible")
    void descartaLoQueNoTraeEventId() {
        ReservationEvent sinId = new ReservationEvent(
                "reservation.created", "  ", Instant.now(), "traza", "corr",
                "ms-andesstay-reservations", new Actor("sub", "Operador", "staff"),
                new ReservationPayload(42L, 7L, null, null, null, null, null,
                        "CREADA", null, null, null, null, null));

        entrada.pipeInput("42", sinId);

        assertThat(salida.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("El recorrido completo de una reserva deja cinco hechos, en orden")
    void recorridoCompleto() {
        entrada.pipeInput("42", evento("reservation.created", null, "CREADA"));
        entrada.pipeInput("42", evento("reservation.confirmed", "CREADA", "CONFIRMADA"));
        entrada.pipeInput("42", evento("reservation.checkin_pending", "CONFIRMADA", "CHECKIN_PENDIENTE"));
        entrada.pipeInput("42", evento("reservation.in_stay", "CHECKIN_PENDIENTE", "EN_ESTADIA"));
        entrada.pipeInput("42", evento("reservation.checked_out", "EN_ESTADIA", "CHECKOUT"));

        assertThat(salida.readValuesToList())
                .extracting(TimelineEvent::type)
                .containsExactly(
                        "reservation.created",
                        "reservation.confirmed",
                        "reservation.checkin_pending",
                        "reservation.in_stay",
                        "reservation.checked_out");
    }

    private static ReservationEvent evento(String tipo, String desde, String hacia) {
        return new ReservationEvent(
                tipo, "evt-1", Instant.parse("2026-11-17T12:00:00Z"), "traza-1", "corr-1",
                "ms-andesstay-reservations",
                new Actor("sub-operador", "Operador", "staff"),
                new ReservationPayload(42L, 7L, "sub-huesped", "huesped@andesstay.cl",
                        LocalDate.of(2026, 3, 10), LocalDate.of(2026, 3, 13), 2,
                        hacia, desde, null, new BigDecimal("135000.00"), "CLP", null));
    }
}
