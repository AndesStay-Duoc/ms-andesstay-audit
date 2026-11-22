package cl.andesstay.audit.streams;

import cl.andesstay.audit.messaging.events.ReservationEvent;
import cl.andesstay.audit.messaging.events.TimelineEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Los Serde de JSON que usa la topologia, en un solo sitio.
 *
 * DOS DECISIONES QUE IMPORTAN:
 *
 * ignoreTypeHeaders(). El productor no manda la cabecera con el nombre de la clase, a proposito:
 * asi cada repositorio tiene su propia copia del record y los paquetes no tienen que coincidir.
 * El consumidor declara el tipo que espera y lo deserializa contra el, que es lo que hace que un
 * polirepo sin libreria compartida siga siendo manejable.
 *
 * Los paquetes de confianza se limitan a cl.andesstay. Antes estaba en "*", que desactiva la
 * proteccion de JsonDeserializer y permite instanciar cualquier clase del classpath a partir de
 * un mensaje. Con un tema interno el riesgo es bajo, pero no es gratis y no hace falta.
 *
 * Y UNA TERCERA: LAS FECHAS EN ISO 8601. Con el ObjectMapper por defecto, lo que esta topologia
 * escribe en audit.timeline llevaba el timestamp como segundos desde 1970 (1795255200.000000000),
 * y el contrato dice "2026-11-21T10:00:00Z". El mapper propio solo cambia como se ESCRIBE: al leer,
 * JavaTimeModule acepta los dos formatos, asi que los mensajes antiguos de reservations.events se
 * siguen procesando.
 */
@Configuration
public class SerdesDeAuditoria {

    private static final String PAQUETES_DE_CONFIANZA = "cl.andesstay.*";

    @Bean
    public JsonSerde<ReservationEvent> reservationEvent() {
        return serdeDe(ReservationEvent.class);
    }

    @Bean
    public JsonSerde<TimelineEvent> timelineEvent() {
        return serdeDe(TimelineEvent.class);
    }

    private static <T> JsonSerde<T> serdeDe(Class<T> tipo) {
        ObjectMapper mapeador = JacksonUtils.enhancedObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        JsonSerde<T> serde = new JsonSerde<>(tipo, mapeador);
        serde.deserializer().addTrustedPackages(PAQUETES_DE_CONFIANZA);
        return serde.ignoreTypeHeaders();
    }
}
