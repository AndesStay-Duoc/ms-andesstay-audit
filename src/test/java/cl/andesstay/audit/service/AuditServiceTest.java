package cl.andesstay.audit.service;

import cl.andesstay.audit.domain.AuditEvent;
import cl.andesstay.audit.messaging.events.Actor;
import cl.andesstay.audit.messaging.events.TimelineEvent;
import cl.andesstay.audit.messaging.events.TimelinePayload;
import cl.andesstay.audit.repository.AuditEventRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Las reglas de auditoria, sin broker y sin base de datos.
 *
 * Que se puedan probar asi es el motivo de haberlas sacado del consumidor: antes vivian dentro de
 * un metodo anotado con @KafkaListener y no habia forma de ejercitarlas sin levantar Kafka.
 */
@ExtendWith(MockitoExtension.class)
class AuditServiceTest {

    @Mock
    private AuditEventRepository repository;

    @InjectMocks
    private AuditService service;

    @Nested
    @DisplayName("Idempotencia")
    class Idempotencia {

        @Test
        @DisplayName("Un hecho nuevo se registra")
        void registraLoNuevo() {
            when(repository.existsByEventId(anyString())).thenReturn(false);

            assertThat(service.registrar(hecho())).isTrue();
            verify(repository).save(any(AuditEvent.class));
        }

        @Test
        @DisplayName("Un hecho ya visto no se vuelve a guardar")
        void descartaElDuplicado() {
            when(repository.existsByEventId("evt-1")).thenReturn(true);

            assertThat(service.registrar(hecho())).isFalse();
            verify(repository, never()).save(any());
        }
    }

    @Nested
    @DisplayName("El mapeo al registro")
    class Mapeo {

        @Test
        @DisplayName("Guarda la transicion completa, de donde venia y adonde fue")
        void guardaLaTransicion() {
            when(repository.existsByEventId(anyString())).thenReturn(false);

            service.registrar(hecho());

            AuditEvent guardado = capturado();
            assertThat(guardado.getEventId()).isEqualTo("evt-1");
            assertThat(guardado.getEventType()).isEqualTo("reservation.confirmed");
            assertThat(guardado.getReservationId()).isEqualTo(42L);
            assertThat(guardado.getUnitId()).isEqualTo(7L);
            assertThat(guardado.getGuestId()).isEqualTo("sub-huesped");
            assertThat(guardado.getStatusBefore()).isEqualTo("CREADA");
            assertThat(guardado.getStatusAfter()).isEqualTo("CONFIRMADA");
            assertThat(guardado.getActorId()).isEqualTo("sub-operador");
            assertThat(guardado.getActorRole()).isEqualTo("Operador");
            assertThat(guardado.getTraceId()).isEqualTo("traza-1");
        }

        @Test
        @DisplayName("Fecha el registro con el momento del hecho, no con el del consumo")
        void fechaConElMomentoDelHecho() {
            when(repository.existsByEventId(anyString())).thenReturn(false);

            service.registrar(hecho());

            LocalDateTime esperado = LocalDateTime.ofInstant(
                    Instant.parse("2026-11-17T12:00:00Z"), ZoneId.systemDefault());
            assertThat(capturado().getOccurredAt()).isEqualTo(esperado);
        }
    }

    @Nested
    @DisplayName("Lo que no se puede auditar")
    class Validacion {

        @Test
        @DisplayName("Un hecho vacio se rechaza")
        void hechoVacio() {
            assertThatThrownBy(() -> service.registrar(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Sin eventId se rechaza, porque no hay idempotencia posible")
        void sinEventId() {
            TimelineEvent sinId = new TimelineEvent("reservation.created", "  ",
                    Instant.now(), "t", "c", "origen", actor(), payload("42"));

            assertThatThrownBy(() -> service.registrar(sinId))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("eventId");
        }

        @Test
        @DisplayName("Sin entidad se rechaza")
        void sinEntidad() {
            TimelineEvent sinEntidad = new TimelineEvent("reservation.created", "evt-9",
                    Instant.now(), "t", "c", "origen", actor(), null);

            assertThatThrownBy(() -> service.registrar(sinEntidad))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Un identificador no numerico se rechaza en el primer intento")
        void identificadorInvalido() {
            TimelineEvent raro = new TimelineEvent("reservation.created", "evt-9",
                    Instant.now(), "t", "c", "origen", actor(), payload("no-es-un-numero"));

            assertThatThrownBy(() -> service.registrar(raro))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("no es numerico");
        }
    }

    // ── Apoyo ────────────────────────────────────────────────────────────────

    private AuditEvent capturado() {
        ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(repository).save(captor.capture());
        return captor.getValue();
    }

    private static Actor actor() {
        return new Actor("sub-operador", "Operador", "staff");
    }

    private static TimelinePayload payload(String entidadId) {
        return new TimelinePayload(TimelinePayload.ENTIDAD_RESERVA, entidadId,
                TimelinePayload.ACCION_CAMBIO_ESTADO, "CREADA", "CONFIRMADA", 7L, "sub-huesped");
    }

    private static TimelineEvent hecho() {
        return new TimelineEvent("reservation.confirmed", "evt-1",
                Instant.parse("2026-11-17T12:00:00Z"), "traza-1", "corr-1",
                "ms-andesstay-reservations", actor(), payload("42"));
    }
}
