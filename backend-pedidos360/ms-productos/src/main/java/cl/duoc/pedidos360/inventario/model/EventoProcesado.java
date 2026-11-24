package cl.duoc.pedidos360.inventario.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.domain.Persistable;

/**
 * Registro de los eventos de pedido que el inventario ya procesó. Kafka entrega "al menos una vez": tras una caída o
 * un rebalanceo el mismo evento puede llegar de nuevo, y sin este registro el stock se descontaría dos veces.
 *
 * <p>Se guarda en la misma transacción que el descuento de stock: o quedan los dos, o ninguno. Implementa
 * {@link Persistable} para que {@code save} haga siempre INSERT: si dos hilos procesan el mismo evento a la vez,
 * la clave primaria rechaza al segundo y su transacción se deshace completa.
 */
@Entity
@Table(name = "eventos_procesados")
@Getter
@Setter
@NoArgsConstructor
public class EventoProcesado implements Persistable<String> {

  @Id
  @Column(name = "evento_id", length = 36)
  private String eventoId;

  @Column(name = "pedido_id", nullable = false)
  private Long pedidoId;

  /** STOCK_DESCONTADO o STOCK_INSUFICIENTE. */
  @Column(length = 30)
  private String resultado;

  @Column(name = "procesado_en", nullable = false)
  private Instant procesadoEn = Instant.now();

  public EventoProcesado(String eventoId, Long pedidoId) {
    this.eventoId = eventoId;
    this.pedidoId = pedidoId;
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
