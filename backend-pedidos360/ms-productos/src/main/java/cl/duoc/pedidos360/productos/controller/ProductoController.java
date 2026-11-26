package cl.duoc.pedidos360.productos.controller;

import cl.duoc.pedidos360.inventario.service.InventarioService;
import cl.duoc.pedidos360.productos.dto.ProductoRequest;
import cl.duoc.pedidos360.productos.dto.ProductoResponse;
import cl.duoc.pedidos360.productos.dto.ReposicionRequest;
import cl.duoc.pedidos360.productos.service.ProductoService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Rutas de ms-productos. Todas exigen JWT válido (SecurityConfig); además:
 * <ul>
 *   <li>Lectura: cualquier usuario autenticado (scope Pedidos360.Read).</li>
 *   <li>Crear producto y reponer stock: rol Admin.</li>
 * </ul>
 * Crear y reponer publican un evento en el tópico de inventario de Kafka (PRODUCTO_CREADO, STOCK_REPUESTO) antes del
 * commit. Si Kafka no lo confirma, no se guarda nada y se responde 503 (GlobalExceptionHandler).
 */
@RestController
@RequestMapping("/api/productos")
public class ProductoController {

  private final ProductoService service;
  private final InventarioService inventarioService;

  public ProductoController(ProductoService service, InventarioService inventarioService) {
    this.service = service;
    this.inventarioService = inventarioService;
  }

  @GetMapping
  @PreAuthorize("hasAuthority('SCOPE_Pedidos360.Read')")
  public List<ProductoResponse> listar() {
    return service.listarActivos();
  }

  @GetMapping("/{id}")
  @PreAuthorize("hasAuthority('SCOPE_Pedidos360.Read')")
  public ProductoResponse obtener(@PathVariable Long id) {
    return service.obtener(id);
  }

  @PostMapping
  @PreAuthorize("hasRole('ADMIN')")
  public ResponseEntity<ProductoResponse> crear(@Valid @RequestBody ProductoRequest req) {
    ProductoResponse creado = service.crear(req);
    return ResponseEntity.created(URI.create("/api/productos/" + creado.id())).body(creado);
  }

  /** Suma unidades al stock (200 · 400 · 404 · 503). Saca al producto de la alerta de stock bajo de la analítica. */
  @PatchMapping("/{id}/stock")
  @PreAuthorize("hasRole('ADMIN')")
  public ProductoResponse reponerStock(@PathVariable Long id, @Valid @RequestBody ReposicionRequest req) {
    return inventarioService.reponer(id, req.cantidad());
  }

}
