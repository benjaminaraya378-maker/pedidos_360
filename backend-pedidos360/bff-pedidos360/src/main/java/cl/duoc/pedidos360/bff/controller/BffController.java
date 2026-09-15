package cl.duoc.pedidos360.bff.controller;

import cl.duoc.pedidos360.bff.dto.DashboardResponse;
import cl.duoc.pedidos360.bff.dto.MeResponse;
import cl.duoc.pedidos360.bff.service.BffService;
import java.util.List;
import java.util.Map;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Rutas del BFF (todas detrás del API Gateway bajo /api/bff/**).
 * <ul>
 *   <li>GET  /api/bff/me            → JWT válido: devuelve identidad, roles y scopes leídos del token.</li>
 *   <li>GET  /api/bff/dashboard     → JWT válido: compone catálogo + pedidos del usuario.</li>
 *   <li>GET  /api/bff/admin/pedidos → rol Admin: todos los pedidos del sistema (403 para Cliente).</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/bff")
public class BffController {

  private final BffService service;

  public BffController(BffService service) {
    this.service = service;
  }

  @GetMapping("/me")
  public MeResponse me(@AuthenticationPrincipal Jwt jwt) {
    return service.me(jwt);
  }

  @GetMapping("/dashboard")
  @PreAuthorize("hasAuthority('SCOPE_Pedidos360.Read')")
  public DashboardResponse dashboard(@AuthenticationPrincipal Jwt jwt) {
    return service.dashboard(jwt);
  }

  @GetMapping("/admin/pedidos")
  @PreAuthorize("hasRole('ADMIN')")
  public List<Map<String, Object>> pedidosAdmin() {
    return service.pedidosAdmin();
  }
}
