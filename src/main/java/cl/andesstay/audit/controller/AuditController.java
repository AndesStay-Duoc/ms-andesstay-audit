package cl.andesstay.audit.controller;

import cl.andesstay.audit.domain.AuditEvent;
import cl.andesstay.audit.repository.AuditEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Solo lectura. Roles permitidos: Admin y Auditor.
 */
@RestController
@RequestMapping("/api/audit")
@RequiredArgsConstructor
public class AuditController {

    private final AuditEventRepository repository;

    /** GET /api/audit/reservations/{id}/timeline — Timeline completo de una reserva */
    @GetMapping("/reservations/{id}/timeline")
    @PreAuthorize("hasAnyRole('Admin', 'Auditor')")
    public ResponseEntity<List<AuditEvent>> getTimeline(@PathVariable Long id) {
        return ResponseEntity.ok(repository.findByReservationIdOrderByOccurredAtAsc(id));
    }

    /** GET /api/audit/events?from=&to= — Eventos en un rango de fechas */
    @GetMapping("/events")
    @PreAuthorize("hasAnyRole('Admin', 'Auditor')")
    public ResponseEntity<List<AuditEvent>> getEvents(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {
        return ResponseEntity.ok(repository.findByOccurredAtBetweenOrderByOccurredAtDesc(from, to));
    }

    /** GET /api/audit/events/actor/{actorId} — Todas las acciones de un usuario */
    @GetMapping("/events/actor/{actorId}")
    @PreAuthorize("hasAnyRole('Admin', 'Auditor')")
    public ResponseEntity<List<AuditEvent>> getByActor(@PathVariable String actorId) {
        return ResponseEntity.ok(repository.findByActorIdOrderByOccurredAtDesc(actorId));
    }

    /** GET /api/audit/events/type/{eventType} */
    @GetMapping("/events/type/{eventType}")
    @PreAuthorize("hasAnyRole('Admin', 'Auditor')")
    public ResponseEntity<List<AuditEvent>> getByType(@PathVariable String eventType) {
        return ResponseEntity.ok(repository.findByEventTypeOrderByOccurredAtDesc(eventType));
    }
}
