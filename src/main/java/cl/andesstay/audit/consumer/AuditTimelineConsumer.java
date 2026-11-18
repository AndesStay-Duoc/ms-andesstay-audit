package cl.andesstay.audit.consumer;

import cl.andesstay.audit.messaging.events.TimelineEvent;
import cl.andesstay.audit.service.AuditService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consume audit.timeline y persiste cada hecho.
 *
 * CIERRA UN CICLO QUE ESTABA ABIERTO. El contrato dice desde el principio que ms-andesstay-audit
 * consume audit.timeline, pero nadie lo hacia: el tema se escribia y se quedaba ahi. Lo que
 * persistia era el consumidor de reservations.events, que ademas reemitia a audit.timeline desde
 * dentro del mismo metodo. Ahora la topologia de Kafka Streams transforma y este consumidor
 * persiste, cada uno con su grupo y sus offsets.
 *
 * ESTA CLASE NO TIENE LOGICA. Recibe, delega y confirma. Esa es toda su responsabilidad: la regla
 * de idempotencia, la validacion y el mapeo viven en AuditService, que se puede probar sin broker.
 *
 * LA CONFIRMACION ES MANUAL y va DESPUES de persistir. Si el servicio lanza, no se confirma el
 * offset: el manejador de errores reintenta y, agotados los intentos, manda el mensaje al tema de
 * descarte y confirma el offset para que la particion siga avanzando. Antes no habia manejador y
 * el catch se limitaba a registrar el error sin confirmar, de modo que un solo mensaje imposible
 * de procesar bloqueaba su particion para siempre.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AuditTimelineConsumer {

    private final AuditService auditService;

    @KafkaListener(
            topics = "${andesstay.kafka.topics.audit-timeline}",
            groupId = "${andesstay.kafka.groups.audit-timeline}",
            containerFactory = "timelineKafkaListenerContainerFactory",
            // Misma bandera que apaga la topologia de Streams: o el servicio habla con Kafka o no
            // lo hace, y no tiene sentido que una mitad arranque sin la otra. Asi el apagado es
            // una sola propiedad y no depende de que el compose acierte con la variable.
            autoStartup = "${andesstay.kafka.enabled:true}")
    public void consumir(ConsumerRecord<String, TimelineEvent> registro, Acknowledgment acuse) {
        TimelineEvent hecho = registro.value();

        log.debug("[Audit] Recibido eventId={} particion={} offset={}",
                hecho == null ? "null" : hecho.eventId(), registro.partition(), registro.offset());

        auditService.registrar(hecho);
        acuse.acknowledge();
    }
}
