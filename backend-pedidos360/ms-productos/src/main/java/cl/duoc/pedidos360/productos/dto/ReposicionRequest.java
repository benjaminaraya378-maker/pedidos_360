package cl.duoc.pedidos360.productos.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/** Cuerpo de PATCH /api/productos/{id}/stock: unidades que se suman al stock actual. */
public record ReposicionRequest(
    @NotNull(message = "es obligatoria") @Positive(message = "debe ser mayor que 0")
    @Max(value = 100000, message = "máximo 100000 por reposición") Integer cantidad) {}
