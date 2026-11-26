package cl.duoc.pedidos360.inventario.event;

import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import java.util.List;

/**
 * Evento de Spring con los movimientos de stock de una transacción. Justo antes del commit,
 * {@code InventarioKafkaProducer} publica cada movimiento en el tópico de inventario y espera la confirmación del
 * broker; si no llega, la transacción se deshace.
 */
public record MovimientosInventarioEvent(List<InventarioEvento> movimientos) {}
