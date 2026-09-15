package cl.duoc.pedidos360.productos.controller;

import cl.duoc.pedidos360.productos.dto.ProductoRequest;
import cl.duoc.pedidos360.productos.dto.ProductoResponse;
import cl.duoc.pedidos360.productos.service.ProductoService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Rutas de ms-productos. Todas exigen JWT válido (SecurityConfig); además:
 * <ul>
 *   <li>Lectura: cualquier usuario autenticado (scope Pedidos360.Read).</li>
 *   <li>Crear producto: rol Admin.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/productos")
public class ProductoController {

  private final ProductoService service;

  public ProductoController(ProductoService service) {
    this.service = service;
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

}
