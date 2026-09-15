package cl.duoc.pedidos360.pedidos.repository;

import cl.duoc.pedidos360.pedidos.model.Pedido;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PedidoRepository extends JpaRepository<Pedido, Long> {
  List<Pedido> findByClienteOidOrderByFechaDesc(String clienteOid);
  List<Pedido> findAllByOrderByFechaDesc();
}
