package cl.duoc.pedidos360.bff.service;

import cl.duoc.pedidos360.bff.client.PedidosClient;
import cl.duoc.pedidos360.bff.client.ProductosClient;
import cl.duoc.pedidos360.bff.dto.DashboardResponse;
import cl.duoc.pedidos360.bff.dto.MeResponse;
import cl.duoc.pedidos360.security.JwtUtils;
import cl.duoc.pedidos360.security.SecurityProperties;
import java.util.List;
import java.util.Map;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Service;

@Service
public class BffService {

  private final ProductosClient productos;
  private final PedidosClient pedidos;
  private final SecurityProperties securityProps;

  public BffService(ProductosClient productos, PedidosClient pedidos, SecurityProperties securityProps) {
    this.productos = productos;
    this.pedidos = pedidos;
    this.securityProps = securityProps;
  }

  public MeResponse me(Jwt jwt) {
    return new MeResponse(
        JwtUtils.name(jwt),
        JwtUtils.email(jwt),
        JwtUtils.userId(jwt),
        JwtUtils.roles(jwt, securityProps.rolesClaim()),
        JwtUtils.scopes(jwt),
        jwt.getIssuer() == null ? "" : jwt.getIssuer().toString(),
        jwt.getAudience(),
        jwt.getExpiresAt() == null ? "" : jwt.getExpiresAt().toString(),
        "bff-pedidos360");
  }

  /** Compone en una sola respuesta lo que el frontend necesita para su portada. */
  public DashboardResponse dashboard(Jwt jwt) {
    List<Map<String, Object>> catalogo = productos.listar();
    long disponibles = catalogo.stream()
        .filter(p -> ((Number) p.getOrDefault("stock", 0)).intValue() > 0)
        .count();
    List<Map<String, Object>> misPedidos = pedidos.listar();
    return new DashboardResponse(me(jwt), disponibles, misPedidos.size(),
        misPedidos.stream().limit(5).toList());
  }

  /** Solo rol Admin: todos los pedidos del sistema (ms-pedidos devuelve todos cuando el token trae rol Admin). */
  public List<Map<String, Object>> pedidosAdmin() {
    return pedidos.listar();
  }
}
