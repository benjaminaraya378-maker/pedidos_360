package cl.duoc.pedidos360.kafka.evento;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Contrato del tópico {@code pedidos360.pedidos} (valor JSON; la clave es el id del pedido).
 *
 * <p>Todos los eventos de un mismo pedido llevan la misma clave, así que caen en la misma partición y se leen en
 * el orden en que ocurrieron (CREADO antes que CONFIRMADO o CANCELADO).
 *
 * @param eventoId  identificador único del evento: los consumidores lo usan para no procesarlo dos veces
 * @param tipo      qué ocurrió con el pedido
 * @param version   versión del contrato, para poder evolucionarlo sin romper a los consumidores
 * @param fecha     cuándo ocurrió
 * @param pedidoId  pedido afectado (también es la clave del mensaje)
 * @param clienteOid cliente dueño del pedido (claim oid). No viajan correo ni nombre: ningún consumidor los necesita
 * @param estado    estado del pedido después del evento
 * @param total     total del pedido
 * @param motivo    explicación del cambio de estado (por ejemplo, qué producto no tenía stock)
 * @param items     productos y cantidades: el inventario los necesita para descontar stock
 */
public record PedidoEvento(
    @NotBlank String eventoId,
    @NotNull TipoEventoPedido tipo,
    @NotNull @Positive Integer version,
    @NotNull @JsonFormat(shape = JsonFormat.Shape.STRING) Instant fecha,
    @NotNull @Positive Long pedidoId,
    @NotBlank String clienteOid,
    @NotBlank String estado,
    @NotNull @PositiveOrZero BigDecimal total,
    String motivo,
    @NotEmpty List<@Valid Item> items) {

  public static final int VERSION_ACTUAL = 1;

  public record Item(
      @NotNull @Positive Long productoId,
      @NotBlank String nombreProducto,
      @NotNull @Positive Integer cantidad,
      @NotNull @PositiveOrZero BigDecimal precioUnitario,
      @NotNull @PositiveOrZero BigDecimal subtotal) {}

  public static PedidoEvento nuevo(TipoEventoPedido tipo, Long pedidoId, String clienteOid, String estado,
                                   BigDecimal total, String motivo, List<Item> items) {
    return new PedidoEvento(UUID.randomUUID().toString(), tipo, VERSION_ACTUAL, Instant.now(), pedidoId, clienteOid,
        estado, total, motivo, items);
  }
}
