package cl.duoc.pedidos360.productos.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

public record ProductoRequest(
    @NotBlank @Size(max = 120) String nombre,
    @Size(max = 500) String descripcion,
    @Size(max = 60) String categoria,
    @NotNull @DecimalMin(value = "0.0", inclusive = true) BigDecimal precio,
    @NotNull @Min(0) Integer stock) {}
