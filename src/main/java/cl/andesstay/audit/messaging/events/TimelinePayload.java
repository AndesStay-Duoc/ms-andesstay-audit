package cl.andesstay.audit.messaging.events;

/**
 * Lo que el timeline necesita saber de cada hecho: que entidad, que accion y entre que estados.
 *
 * Sigue la forma que describe infra/docs/contracts/events/kafka.md para audit.timeline. El
 * contrato incluye ademas ip y userAgent; aqui no se rellenan porque la topologia de Kafka Streams
 * transforma un evento ya publicado y no tiene delante la peticion HTTP que lo origino. Anotar un
 * valor inventado seria peor que dejarlos fuera: la auditoria vale por lo que puede sostener.
 *
 * @param entidad   tipo de la entidad afectada, hoy siempre Reservation
 * @param entidadId identificador de la entidad
 * @param accion    CREACION la primera vez, CAMBIO_ESTADO en cada transicion posterior
 * @param desde     estado del que venia, o null en la creacion
 * @param hacia     estado en el que quedo
 * @param unitId    unidad implicada, util para cruzar el timeline con el catalogo
 * @param guestId   huesped implicado, o null si reservo recepcion a su nombre
 */
public record TimelinePayload(
        String entidad,
        String entidadId,
        String accion,
        String desde,
        String hacia,
        Long unitId,
        String guestId) {

    public static final String ENTIDAD_RESERVA = "Reservation";
    public static final String ACCION_CREACION = "CREACION";
    public static final String ACCION_CAMBIO_ESTADO = "CAMBIO_ESTADO";
}
