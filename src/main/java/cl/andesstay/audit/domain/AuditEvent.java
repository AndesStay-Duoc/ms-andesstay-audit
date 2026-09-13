package cl.andesstay.audit.domain;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "AUDIT_EVENTS")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class AuditEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "audit_seq")
    @SequenceGenerator(name = "audit_seq", sequenceName = "SEQ_AUDIT_EVENTS", allocationSize = 1)
    private Long id;

    @Column(name = "EVENT_ID", nullable = false, unique = true, length = 36)
    private String eventId;

    @Column(name = "EVENT_TYPE", nullable = false, length = 50)
    private String eventType;   // CREATED, CONFIRMED, CHECKIN, CHECKOUT, CANCELLED

    @Column(name = "RESERVATION_ID", nullable = false)
    private Long reservationId;

    @Column(name = "UNIT_ID")
    private Long unitId;

    @Column(name = "GUEST_ID", length = 100)
    private String guestId;

    @Column(name = "STATUS_BEFORE", length = 30)
    private String statusBefore;

    @Column(name = "STATUS_AFTER", length = 30)
    private String statusAfter;

    @Column(name = "ACTOR_ID", length = 100)
    private String actorId;

    @Column(name = "ACTOR_ROLE", length = 50)
    private String actorRole;

    @Column(name = "TRACE_ID", length = 36)
    private String traceId;

    @Column(name = "OCCURRED_AT", nullable = false)
    private LocalDateTime occurredAt;

    @Column(name = "PERSISTED_AT", nullable = false)
    private LocalDateTime persistedAt;

    @PrePersist
    protected void onCreate() {
        persistedAt = LocalDateTime.now();
    }
}
