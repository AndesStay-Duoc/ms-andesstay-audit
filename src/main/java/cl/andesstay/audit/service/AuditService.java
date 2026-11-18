package cl.andesstay.audit.service;

import cl.andesstay.audit.domain.AuditEvent;
import cl.andesstay.audit.messaging.events.TimelineEvent;
import cl.andesstay.audit.repository.AuditEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * La logica de auditoria, separada de la mensajeria.
 *
 * El consumidor de Kafka no sabe nada de esto y este servicio no sabe nada de Kafka: recibe un
 * hecho ya deserializado y decide que hacer con el. Esa separacion es lo que permite probar la
 * regla de idempotencia sin levantar un broker, y lo que permitiria alimentar el timeline desde
 * otra fuente sin tocar una linea de dominio.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {

    private final AuditEventRepository repository;

    /**
     * Registra un hecho en el timeline, una sola vez.
     *
     * LA IDEMPOTENCIA ES NECESARIA, no una precaucion. Kafka entrega al menos una vez: un
     * reequilibrio de particiones o un reinicio antes de confirmar el offset hacen que el mismo
     * mensaje vuelva. Descartarlo por eventId es lo que convierte esa garantia en "exactamente una
     * vez" desde el punto de vista del negocio.
     *
     * @return true si el hecho se registro, false si ya estaba
     */
    @Transactional
    public boolean registrar(TimelineEvent hecho) {
        validar(hecho);

        if (repository.existsByEventId(hecho.eventId())) {
            log.warn("[Audit] Hecho duplicado ignorado eventId={} tipo={}",
                    hecho.eventId(), hecho.type());
            return false;
        }

        AuditEvent registro = aRegistro(hecho);
        repository.save(registro);

        log.info("[Audit] Registrado {} de la reserva {}: {} → {} por actor={}",
                registro.getEventType(), registro.getReservationId(),
                registro.getStatusBefore(), registro.getStatusAfter(), registro.getActorId());
        return true;
    }

    /**
     * Lo que el timeline no puede aceptar de ninguna manera.
     *
     * Son invariantes de la tabla, no gustos: sin eventId no hay idempotencia posible y sin
     * reserva el registro no pertenece a ninguna linea de tiempo. Se lanza IllegalArgumentException
     * a proposito, porque es un fallo del mensaje y no del sistema: el manejador de errores del
     * consumidor lo trata como irrecuperable y lo manda a la cola de descarte en el primer intento,
     * sin gastar reintentos en algo que nunca va a mejorar.
     */
    private void validar(TimelineEvent hecho) {
        if (hecho == null) {
            throw new IllegalArgumentException("El hecho llego vacio.");
        }
        if (hecho.eventId() == null || hecho.eventId().isBlank()) {
            throw new IllegalArgumentException("El hecho no trae eventId: sin el no hay idempotencia.");
        }
        if (hecho.payload() == null || hecho.payload().entidadId() == null) {
            throw new IllegalArgumentException("El hecho no identifica ninguna entidad.");
        }
        try {
            Long.parseLong(hecho.payload().entidadId());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "El identificador de la entidad no es numerico: " + hecho.payload().entidadId());
        }
    }

    private AuditEvent aRegistro(TimelineEvent hecho) {
        var p = hecho.payload();
        var actor = hecho.actor();

        return AuditEvent.builder()
                .eventId(hecho.eventId())
                .eventType(hecho.type())
                .reservationId(Long.parseLong(p.entidadId()))
                .unitId(p.unitId())
                .guestId(p.guestId())
                // Antes esta columna quedaba siempre vacia porque el evento no llevaba el estado
                // anterior, y un timeline que no dice de donde venia la reserva no muestra
                // transiciones: muestra fotos sueltas.
                .statusBefore(p.desde())
                .statusAfter(p.hacia())
                .actorId(actor == null ? null : actor.userId())
                .actorRole(actor == null ? null : actor.role())
                .traceId(hecho.traceId())
                .occurredAt(momentoDelHecho(hecho.timestamp()))
                .build();
    }

    /**
     * Cuando ocurrio, no cuando se consumio.
     *
     * Antes se guardaba LocalDateTime.now() del consumidor. Con el consumo al dia la diferencia es
     * de milisegundos, pero basta un rato de retraso, una reposicion de offsets o una relectura del
     * tema desde el principio para que el timeline entero quede fechado en el momento de la
     * relectura. El instante del hecho viene en el evento y es el unico que no miente.
     */
    private LocalDateTime momentoDelHecho(Instant instante) {
        Instant efectivo = instante == null ? Instant.now() : instante;
        return LocalDateTime.ofInstant(efectivo, ZoneId.systemDefault());
    }
}
