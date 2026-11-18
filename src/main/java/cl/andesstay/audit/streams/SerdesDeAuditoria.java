package cl.andesstay.audit.streams;

import cl.andesstay.audit.messaging.events.ReservationEvent;
import cl.andesstay.audit.messaging.events.TimelineEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
        JsonSerde<T> serde = new JsonSerde<>(tipo);
        serde.deserializer().addTrustedPackages(PAQUETES_DE_CONFIANZA);
        return serde.ignoreTypeHeaders();
    }
}
