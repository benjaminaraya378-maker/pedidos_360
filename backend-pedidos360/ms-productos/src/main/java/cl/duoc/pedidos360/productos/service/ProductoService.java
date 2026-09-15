package cl.duoc.pedidos360.productos.service;

import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.productos.dto.ProductoRequest;
import cl.duoc.pedidos360.productos.dto.ProductoResponse;
import cl.duoc.pedidos360.productos.model.Producto;
import cl.duoc.pedidos360.productos.repository.ProductoRepository;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ProductoService {

  private final ProductoRepository repository;

  public ProductoService(ProductoRepository repository) {
    this.repository = repository;
  }

  @Transactional(readOnly = true)
  public List<ProductoResponse> listarActivos() {
    return repository.findByActivoTrueOrderByNombreAsc().stream().map(ProductoResponse::de).toList();
  }

  @Transactional(readOnly = true)
  public ProductoResponse obtener(Long id) {
    return ProductoResponse.de(buscar(id));
  }

  public ProductoResponse crear(ProductoRequest req) {
    Producto p = new Producto();
    aplicar(p, req);
    return ProductoResponse.de(repository.save(p));
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
