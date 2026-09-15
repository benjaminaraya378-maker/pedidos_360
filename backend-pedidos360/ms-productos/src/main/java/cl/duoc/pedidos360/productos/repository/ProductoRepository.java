package cl.duoc.pedidos360.productos.repository;

import cl.duoc.pedidos360.productos.model.Producto;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductoRepository extends JpaRepository<Producto, Long> {
  List<Producto> findByActivoTrueOrderByNombreAsc();
}
