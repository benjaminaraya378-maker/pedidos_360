package cl.duoc.pedidos360.pedidos.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.Length;
import org.springframework.data.domain.Persistable;

/**
 * Fila del outbox de Kafka: un evento que ms-pedidos debe publicar y que Kafka todavía no confirmó.
 *
 * <p>Se inserta en la <b>misma transacción</b> que guarda el pedido: o quedan los dos en la base, o ninguno. Así un
 * pedido nunca queda guardado sin su evento PEDIDO_CREADO, aunque Kafka esté caído justo en ese momento. La fila se
 * borra cuando Kafka confirma el envío; mientras exista, el relé de {@code OutboxKafka} la sigue intentando.
 *
 * <p>Implementa {@link Persistable} para que {@code save} haga siempre INSERT (el id lo trae el evento y no lo genera
 * la base). Los cambios posteriores (sumar un intento, borrar) se hacen con consultas de actualización del
 * repositorio, sin cargar la entidad.
 */
@Entity
@Table(name = "eventos_kafka_pendientes",
    indexes = @Index(name = "idx_eventos_kafka_pendientes_creado_en", columnList = "creado_en"))
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EventoKafkaPendiente implements Persistable<String> {

  /** Mismo eventoId del evento: los consumidores lo usan para descartar un envío repetido. */
  @Id
  @Column(name = "evento_id", length = 36)
  private String eventoId;

  @Column(nullable = false, length = 200)
  private String topico;

  /** Clave del mensaje (el id del pedido): fija la partición y con ella el orden de los eventos del pedido. */
  @Column(nullable = false, length = 100)
  private String clave;

  /** El PedidoEvento en JSON. Length.LONG32: Hibernate crea la columna como LONGTEXT en MySQL. */
  @Column(nullable = false, length = Length.LONG32)
  private String payload;

  @Column(name = "creado_en", nullable = false)
  private Instant creadoEn;

  /** Envíos fallidos hasta ahora (el intento inmediato después del commit cuenta como el primero). */
  @Column(nullable = false)
  private int intentos;

  /** Motivo del último fallo, para revisarlo en la tabla sin buscar en el log. */
  @Column(name = "ultimo_error", length = 500)
  private String ultimoError;

  public EventoKafkaPendiente(String eventoId, String topico, String clave, String payload, Instant creadoEn) {
    this.eventoId = eventoId;
    this.topico = topico;
    this.clave = clave;
    this.payload = payload;
    this.creadoEn = creadoEn;
  }

  @Override
  public String getId() {
    return eventoId;
  }

  @Override
  @Transient
  public boolean isNew() {
    return true;
  }
}
