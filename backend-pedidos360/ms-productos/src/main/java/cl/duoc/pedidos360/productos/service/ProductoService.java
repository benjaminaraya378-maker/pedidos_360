package cl.duoc.pedidos360.productos.service;

import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.inventario.InventarioProperties;
import cl.duoc.pedidos360.inventario.event.InventarioEventos;
import cl.duoc.pedidos360.inventario.event.MovimientosInventarioEvent;
import cl.duoc.pedidos360.productos.dto.ProductoRequest;
import cl.duoc.pedidos360.productos.dto.ProductoResponse;
import cl.duoc.pedidos360.productos.model.Producto;
import cl.duoc.pedidos360.productos.repository.ProductoRepository;
import java.util.List;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ProductoService {

  private final ProductoRepository repository;
  private final ApplicationEventPublisher eventos;
  private final InventarioProperties inventario;

  public ProductoService(ProductoRepository repository, ApplicationEventPublisher eventos,
                         InventarioProperties inventario) {
    this.repository = repository;
    this.eventos = eventos;
    this.inventario = inventario;
  }

  @Transactional(readOnly = true)
  public List<ProductoResponse> listarActivos() {
    return repository.findByActivoTrueOrderByNombreAsc().stream().map(ProductoResponse::de).toList();
  }

  @Transactional(readOnly = true)
  public ProductoResponse obtener(Long id) {
    return ProductoResponse.de(buscar(id));
  }

  /**
   * Crea el producto y anuncia su stock inicial en Kafka (PRODUCTO_CREADO) antes del commit. Si Kafka no confirma el
   * evento, el producto no se guarda (el controlador responde 503).
   */
  public ProductoResponse crear(ProductoRequest req) {
    Producto p = new Producto();
    aplicar(p, req);
    Producto guardado = repository.save(p);
    eventos.publishEvent(new MovimientosInventarioEvent(
        List.of(InventarioEventos.alta(guardado, inventario.umbralStockBajo()))));
    return ProductoResponse.de(guardado);
  }

  private Producto buscar(Long id) {
    return repository.findById(id)
        .orElseThrow(() -> new ResourceNotFoundException("Producto " + id + " no existe"));
  }

  private static void aplicar(Producto p, ProductoRequest req) {
    p.setNombre(req.nombre().trim());
    p.setDescripcion(req.descripcion());
    p.setCategoria(req.categoria() == null || req.categoria().isBlank() ? "General" : req.categoria().trim());
    p.setPrecio(req.precio());
    p.setStock(req.stock());
  }
}
