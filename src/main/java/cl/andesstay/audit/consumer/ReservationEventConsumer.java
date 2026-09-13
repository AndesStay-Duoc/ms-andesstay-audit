package cl.andesstay.audit.consumer;

import cl.andesstay.audit.domain.AuditEvent;
import cl.andesstay.audit.repository.AuditEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReservationEventConsumer {

    private final AuditEventRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${andesstay.kafka.topics.audit-timeline}")
    private String auditTimelineTopic;

    @KafkaListener(
        topics = "${andesstay.kafka.topics.reservations-events}",
        groupId = "audit-consumer-group"
    )
    public void consume(ConsumerRecord<String, Map<String, Object>> record,
                        Acknowledgment acknowledgment) {
        Map<String, Object> event = record.value();
        String eventId = (String) event.get("eventId");

        try {
            // Idempotencia: si el evento ya fue procesado, ignorar
            if (repository.existsByEventId(eventId)) {
                log.warn("[Audit] Evento duplicado ignorado eventId={}", eventId);
                acknowledgment.acknowledge();
                return;
            }

            AuditEvent auditEvent = AuditEvent.builder()
                    .eventId(eventId)
                    .eventType((String) event.get("eventType"))
                    .reservationId(toLong(event.get("reservationId")))
                    .unitId(toLong(event.get("unitId")))
                    .guestId((String) event.get("guestId"))
                    .statusAfter((String) event.get("status"))
                    .actorId((String) event.get("actorId"))
                    .actorRole((String) event.get("actorRole"))
                    .traceId((String) event.get("traceId"))
                    .occurredAt(LocalDateTime.now())
                    .build();

            repository.save(auditEvent);

            // Publicar al tópico audit.timeline (historial quién/qué/cuándo)
            kafkaTemplate.send(auditTimelineTopic,
                    String.valueOf(auditEvent.getReservationId()), event)
                .whenComplete((r, ex) -> {
                    if (ex != null) log.warn("[Audit] No se pudo publicar en audit.timeline: {}", ex.getMessage());
                });

            acknowledgment.acknowledge();

            log.info("[Audit] Evento persistido y publicado en audit.timeline → eventType={} reservationId={} actor={}",
                    auditEvent.getEventType(), auditEvent.getReservationId(), auditEvent.getActorId());

        } catch (Exception e) {
            log.error("[Audit] Error procesando evento Kafka eventId={}: {}", eventId, e.getMessage());
            // No hacemos acknowledge → Kafka reintentará
        }
    }

    private Long toLong(Object value) {
        if (value == null) return null;
        if (value instanceof Number n) return n.longValue();
        return Long.parseLong(value.toString());
    }
}
