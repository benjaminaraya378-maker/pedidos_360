package cl.duoc.pedidos360.pedidos.controller;

import cl.duoc.pedidos360.pedidos.dto.CrearPedidoRequest;
import cl.duoc.pedidos360.pedidos.dto.PedidoResponse;
import cl.duoc.pedidos360.pedidos.service.PedidoService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Rutas de ms-pedidos. Todas exigen JWT válido; además:
 * <ul>
 *   <li>Crear pedido: scope Pedidos360.Write.</li>
 *   <li>Listar / ver: scope Pedidos360.Read (un cliente solo ve sus pedidos; Admin ve todos).</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/pedidos")
public class PedidoController {

  private final PedidoService service;

  public PedidoController(PedidoService service) {
    this.service = service;
  }

  @PostMapping
  @PreAuthorize("hasAuthority('SCOPE_Pedidos360.Write')")
  public ResponseEntity<PedidoResponse> crear(@AuthenticationPrincipal Jwt jwt,
                                              @Valid @RequestBody CrearPedidoRequest req) {
    PedidoResponse creado = service.crear(jwt, req);
    return ResponseEntity.created(URI.create("/api/pedidos/" + creado.id())).body(creado);
  }

  @GetMapping
  @PreAuthorize("hasAuthority('SCOPE_Pedidos360.Read')")
  public List<PedidoResponse> listar(@AuthenticationPrincipal Jwt jwt) {
    return service.listar(jwt);
  }

  @GetMapping("/{id}")
  @PreAuthorize("hasAuthority('SCOPE_Pedidos360.Read')")
  public PedidoResponse obtener(@AuthenticationPrincipal Jwt jwt, @PathVariable Long id) {
    return service.obtener(jwt, id);
  }

}
