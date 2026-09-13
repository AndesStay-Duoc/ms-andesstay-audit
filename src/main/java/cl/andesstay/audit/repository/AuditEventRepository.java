package cl.andesstay.audit.repository;

import cl.andesstay.audit.domain.AuditEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import java.time.LocalDateTime;
import java.util.List;

public interface AuditEventRepository extends JpaRepository<AuditEvent, Long> {

    List<AuditEvent> findByReservationIdOrderByOccurredAtAsc(Long reservationId);

    List<AuditEvent> findByActorIdOrderByOccurredAtDesc(String actorId);

    List<AuditEvent> findByOccurredAtBetweenOrderByOccurredAtDesc(
            LocalDateTime from, LocalDateTime to);

    List<AuditEvent> findByEventTypeOrderByOccurredAtDesc(String eventType);

    boolean existsByEventId(String eventId);  // Idempotencia: evitar duplicados
}
