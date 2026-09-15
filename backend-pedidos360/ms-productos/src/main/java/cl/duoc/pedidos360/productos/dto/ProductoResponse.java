package cl.duoc.pedidos360.productos.dto;

import cl.duoc.pedidos360.productos.model.Producto;
import java.math.BigDecimal;

public record ProductoResponse(
    Long id,
    String nombre,
    String descripcion,
    String categoria,
    BigDecimal precio,
    Integer stock,
    Boolean activo) {

  public static ProductoResponse de(Producto p) {
    return new ProductoResponse(p.getId(), p.getNombre(), p.getDescripcion(), p.getCategoria(),
        p.getPrecio(), p.getStock(), p.getActivo());
  }
}
