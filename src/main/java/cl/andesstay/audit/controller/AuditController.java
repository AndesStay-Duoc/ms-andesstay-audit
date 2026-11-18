package cl.andesstay.audit.controller;

import cl.andesstay.audit.ApiPaths;

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
 *
 * LA VERSION VA UNA SOLA VEZ. ApiPaths.AUDIT ya vale "/api/v1/audit", asi que los @GetMapping
 * cuelgan de el sin repetir "/v1": antes lo repetian y las rutas reales eran
 * /api/v1/audit/v1/events, que ni el contrato ni el frontend ni el gateway esperaban. La version
 * se declara en ApiPaths y en ningun otro sitio, que es justo lo que esa clase promete.
 */
@RestController
@RequestMapping(ApiPaths.AUDIT)
@RequiredArgsConstructor
public class AuditController {

    private final AuditEventRepository repository;

    /** GET /api/v1/audit/reservations/{id}/timeline — Timeline completo de una reserva */
    @GetMapping("/reservations/{id}/timeline")
    @PreAuthorize("hasAnyRole('Admin', 'Auditor')")
    public ResponseEntity<List<AuditEvent>> getTimeline(@PathVariable Long id) {
        return ResponseEntity.ok(repository.findByReservationIdOrderByOccurredAtAsc(id));
    }

    /** GET /api/v1/audit/events?from=&to= — Eventos en un rango de fechas */
    @GetMapping("/events")
    @PreAuthorize("hasAnyRole('Admin', 'Auditor')")
    public ResponseEntity<List<AuditEvent>> getEvents(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {
        return ResponseEntity.ok(repository.findByOccurredAtBetweenOrderByOccurredAtDesc(from, to));
    }

    /** GET /api/v1/audit/events/actor/{actorId} — Todas las acciones de un usuario */
    @GetMapping("/events/actor/{actorId}")
    @PreAuthorize("hasAnyRole('Admin', 'Auditor')")
    public ResponseEntity<List<AuditEvent>> getByActor(@PathVariable String actorId) {
        return ResponseEntity.ok(repository.findByActorIdOrderByOccurredAtDesc(actorId));
    }

    /** GET /api/v1/audit/events/type/{eventType} — Eventos de un tipo */
    @GetMapping("/events/type/{eventType}")
    @PreAuthorize("hasAnyRole('Admin', 'Auditor')")
    public ResponseEntity<List<AuditEvent>> getByType(@PathVariable String eventType) {
        return ResponseEntity.ok(repository.findByEventTypeOrderByOccurredAtDesc(eventType));
    }
}
