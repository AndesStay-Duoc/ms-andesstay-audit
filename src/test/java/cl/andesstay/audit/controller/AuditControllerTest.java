package cl.andesstay.audit.controller;

import cl.andesstay.audit.config.SecurityConfig;
import cl.andesstay.audit.domain.AuditEvent;
import cl.andesstay.audit.repository.AuditEventRepository;
import cl.andesstay.audit.web.ManejadorDeErrores;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Las cuatro rutas de auditoria, tal como las publica el contrato.
 *
 * El valor de esta clase esta en las rutas literales: durante un tiempo el controlador repitio la
 * version y servia /api/v1/audit/v1/events, de modo que el frontend y el gateway recibian 404 en
 * las rutas correctas. Escribir aqui la ruta completa a mano, y no derivarla de ApiPaths, es
 * deliberado: si alguien vuelve a anteponer un prefijo, estas pruebas fallan.
 *
 * El JwtDecoder se simula porque si no el contexto intentaria descargar las claves publicas de
 * Entra ID al arrancar, y los tests dejarian de funcionar sin red.
 */
@WebMvcTest(AuditController.class)
@Import({SecurityConfig.class, ManejadorDeErrores.class})
class AuditControllerTest {

    private static final String BASE = "/api/v1/audit";

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AuditEventRepository repositorio;

    @MockBean
    private JwtDecoder jwtDecoder;

    private static MockHttpServletRequestBuilder con(MockHttpServletRequestBuilder peticion, String rol) {
        return peticion.with(jwt().authorities(new SimpleGrantedAuthority("ROLE_" + rol)));
    }

    private static AuditEvent unEvento() {
        return AuditEvent.builder()
                .eventId("evt-1")
                .eventType("reservation.created")
                .reservationId(7L)
                .actorId("sub-123")
                .actorRole("HUESPED")
                .occurredAt(LocalDateTime.now())
                .build();
    }

    @Nested
    @DisplayName("Las rutas responden donde el contrato dice")
    class Rutas {

        @Test
        @DisplayName("GET /api/v1/audit/reservations/{id}/timeline devuelve el timeline")
        void timeline() throws Exception {
            when(repositorio.findByReservationIdOrderByOccurredAtAsc(anyLong()))
                    .thenReturn(List.of(unEvento()));

            mockMvc.perform(con(get(BASE + "/reservations/7/timeline"), "Auditor"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$[0].eventId").value("evt-1"));
        }

        @Test
        @DisplayName("GET /api/v1/audit/events filtra por rango de fechas")
        void eventosPorRango() throws Exception {
            when(repositorio.findByOccurredAtBetweenOrderByOccurredAtDesc(any(), any()))
                    .thenReturn(List.of(unEvento()));

            mockMvc.perform(con(get(BASE + "/events")
                            .param("from", "2026-11-01T00:00:00")
                            .param("to", "2026-11-30T23:59:59"), "Admin"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$[0].reservationId").value(7));
        }

        @Test
        @DisplayName("GET /api/v1/audit/events/actor/{actorId} filtra por usuario")
        void eventosPorActor() throws Exception {
            when(repositorio.findByActorIdOrderByOccurredAtDesc(anyString()))
                    .thenReturn(List.of(unEvento()));

            mockMvc.perform(con(get(BASE + "/events/actor/sub-123"), "Auditor"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$[0].actorId").value("sub-123"));
        }

        @Test
        @DisplayName("GET /api/v1/audit/events/type/{eventType} filtra por tipo")
        void eventosPorTipo() throws Exception {
            when(repositorio.findByEventTypeOrderByOccurredAtDesc(anyString()))
                    .thenReturn(List.of(unEvento()));

            mockMvc.perform(con(get(BASE + "/events/type/reservation.created"), "Admin"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$[0].eventType").value("reservation.created"));
        }
    }

    @Nested
    @DisplayName("La version no se repite")
    class VersionUnica {

        /**
         * La regresion concreta que se arreglo. Si alguien vuelve a poner "/v1" en los
         * {@code @GetMapping}, esta ruta pasaria a responder 200 y la prueba fallaria.
         */
        @Test
        @DisplayName("La ruta con la version duplicada ya no existe")
        void rutaDuplicadaNoResponde() throws Exception {
            mockMvc.perform(con(get(BASE + "/v1/events")
                            .param("from", "2026-11-01T00:00:00")
                            .param("to", "2026-11-30T23:59:59"), "Admin"))
                    .andExpect(status().isNotFound());
        }
    }

    @Nested
    @DisplayName("Solo Admin y Auditor pueden leer")
    class Autorizacion {

        @Test
        @DisplayName("Sin token responde 401")
        void sinToken() throws Exception {
            mockMvc.perform(get(BASE + "/events/type/reservation.created"))
                    .andExpect(status().isUnauthorized());
        }

        @Test
        @DisplayName("Un Cliente recibe 403")
        void rolSinPermiso() throws Exception {
            mockMvc.perform(con(get(BASE + "/events/type/reservation.created"), "Cliente"))
                    .andExpect(status().isForbidden());
        }
    }
}
