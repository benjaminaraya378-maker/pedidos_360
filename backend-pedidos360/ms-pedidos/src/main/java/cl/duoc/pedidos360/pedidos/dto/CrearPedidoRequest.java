package cl.duoc.pedidos360.pedidos.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;

public record CrearPedidoRequest(
    @NotEmpty @Valid List<ItemRequest> items,
    @Size(max = 300) String observacion) {

  public record ItemRequest(@NotNull Long productoId, @NotNull @Min(1) Integer cantidad) {}
}
