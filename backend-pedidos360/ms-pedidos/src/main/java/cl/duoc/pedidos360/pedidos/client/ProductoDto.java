package cl.duoc.pedidos360.pedidos.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;

/** Proyección de la respuesta de ms-productos. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProductoDto(Long id, String nombre, BigDecimal precio, Integer stock, Boolean activo) {}
