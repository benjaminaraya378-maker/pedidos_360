package cl.duoc.pedidos360.kafka.evento;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Instant;

/**
 * Contrato del tópico {@code pedidos360.inventario} (valor JSON; la clave es el id del producto). Lo publica
 * ms-productos; ms-pedidos lo consume para confirmar o cancelar pedidos.
 *
 * <p>Cada evento trae el stock actual del producto, por eso el tópico se compacta por clave: siempre queda al menos
 * el último evento de cada producto, que es su stock vigente.
 *
 * @param eventoId        identificador único del evento
 * @param tipo            qué le pasó al stock
 * @param version         versión del contrato
 * @param fecha           cuándo ocurrió
 * @param productoId      producto afectado (también es la clave del mensaje)
 * @param nombreProducto  nombre legible, para la analítica y los logs
 * @param cantidad        unidades descontadas, solicitadas sin stock o repuestas
 * @param stockAnterior   stock antes del movimiento
 * @param stockActual     stock después del movimiento
 * @param umbralStockBajo con este stock o menos el producto se considera con stock bajo
 * @param stockBajo       stockActual <= umbralStockBajo
 * @param pedidoId        pedido que originó el movimiento (null en altas y reposiciones)
 * @param motivo          explicación legible del movimiento
 */
public record InventarioEvento(
    @NotBlank @Size(max = 36) String eventoId,
    @NotNull TipoEventoInventario tipo,
    @NotNull @Positive Integer version,
    @NotNull @JsonFormat(shape = JsonFormat.Shape.STRING) Instant fecha,
    @NotNull @Positive Long productoId,
    @NotBlank String nombreProducto,
    @NotNull @PositiveOrZero Integer cantidad,
    @NotNull @PositiveOrZero Integer stockAnterior,
    @NotNull @PositiveOrZero Integer stockActual,
    @NotNull @PositiveOrZero Integer umbralStockBajo,
    boolean stockBajo,
    Long pedidoId,
    String motivo) {}
