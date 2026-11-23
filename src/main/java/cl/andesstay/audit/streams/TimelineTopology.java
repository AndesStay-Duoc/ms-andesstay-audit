package cl.andesstay.audit.streams;

import cl.andesstay.audit.messaging.events.ReservationEvent;
import cl.andesstay.audit.messaging.events.TimelineEvent;
import cl.andesstay.audit.messaging.events.TimelinePayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * La topologia de Kafka Streams que convierte eventos de reserva en entradas de timeline.
 *
 * ES EL PATRON Source → Process → Sink: lee de un tema, transforma y escribe en otro, sin estado.
 * Kafka Streams se encarga del consumo, los offsets, el reparto entre instancias y el reintento;
 * aqui solo se declara QUE transformacion ocurre.
 *
 * POR QUE STREAMS Y NO UN @KafkaListener. Antes esta reemision vivia dentro del consumidor: el
 * mismo metodo persistia en la base y publicaba en audit.timeline con un KafkaTemplate. Eso
 * mezclaba dominio e infraestructura y, peor, ataba las dos cosas a la misma transaccion logica
 * sin que ninguna de las dos fuera transaccional de verdad: si la publicacion fallaba despues del
 * save, el timeline quedaba incompleto y nadie lo sabia. Separarlas deja una topologia que solo
 * transforma y un consumidor que solo persiste, y cada una falla y se reintenta por su cuenta.
 *
 * LA CLAVE NO SE TOCA. Entra y sale el reservationId, asi que los hechos de una misma reserva
 * siguen cayendo en la misma particion y se consumen en orden. Es tambien la clave por la que se
 * compacta audit.timeline: dentro de sus treinta dias deja el hecho mas reciente de cada reserva.
 * Pasada la retencion no queda ninguno, porque la politica es compact,delete; el historial
 * completo lo guarda AuditTimelineConsumer en AUDIT_EVENTS.
 *
 * EL FILTRO descarta lo que no se puede auditar: un evento sin payload o sin reserva no identifica
 * ninguna entidad, y registrarlo solo ensuciaria el timeline. Se deja constancia en el log, que es
 * donde corresponde mirar si el contrato del productor cambia.
 */
@Slf4j
@Configuration
public class TimelineTopology {

    @Value("${andesstay.kafka.topics.reservations-events}")
    private String temaDeEventos;

    @Value("${andesstay.kafka.topics.audit-timeline}")
    private String temaDeTimeline;

    @Bean
    public KStream<String, TimelineEvent> timelineStream(
            StreamsBuilder builder,
            SerdesDeAuditoria serdes) {

        KStream<String, TimelineEvent> timeline = builder
                .stream(temaDeEventos, Consumed.with(Serdes.String(), serdes.reservationEvent()))
                .peek((clave, evento) -> log.debug(
                        "[Streams] Entra {} de la reserva {}", evento == null ? "null" : evento.type(), clave))
                .filter((clave, evento) -> esAuditable(clave, evento))
                .mapValues(TimelineTopology::aEntradaDeTimeline)
                .peek((clave, entrada) -> log.info(
                        "[Streams] {} de la reserva {} → {}", entrada.type(), clave, "audit.timeline"));

        timeline.to(temaDeTimeline, Produced.with(Serdes.String(), serdes.timelineEvent()));
        return timeline;
    }

    /** Un hecho sin entidad identificable no se puede auditar. */
    private static boolean esAuditable(String clave, ReservationEvent evento) {
        if (evento == null || evento.payload() == null || evento.payload().reservationId() == null) {
            log.warn("[Streams] Evento descartado por no identificar ninguna reserva: clave={}", clave);
            return false;
        }
        if (evento.eventId() == null || evento.eventId().isBlank()) {
            log.warn("[Streams] Evento descartado por no traer eventId: clave={} tipo={}",
                    clave, evento.type());
            return false;
        }
        return true;
    }

    /**
     * La transformacion: del hecho de negocio al registro de auditoria.
     *
     * La accion se deduce del estado anterior y no del tipo del evento: si no venia de ningun
     * estado, es que la reserva acaba de nacer. Asi la regla sigue valiendo aunque el catalogo de
     * tipos crezca.
     */
    private static TimelineEvent aEntradaDeTimeline(ReservationEvent evento) {
        var p = evento.payload();
        String accion = p.estadoAnterior() == null
                ? TimelinePayload.ACCION_CREACION
                : TimelinePayload.ACCION_CAMBIO_ESTADO;

        return new TimelineEvent(
                evento.type(),
                evento.eventId(),
                evento.timestamp(),
                evento.traceId(),
                evento.correlationId(),
                evento.source(),
                evento.actor(),
                new TimelinePayload(
                        TimelinePayload.ENTIDAD_RESERVA,
                        String.valueOf(p.reservationId()),
                        accion,
                        p.estadoAnterior(),
                        p.estado(),
                        p.unitId(),
                        p.guestId()));
    }
}
