package cl.duoc.pedidos360.pedidos.repository;

import cl.duoc.pedidos360.pedidos.model.Pedido;
import jakarta.persistence.LockModeType;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PedidoRepository extends JpaRepository<Pedido, Long> {
  List<Pedido> findByClienteOidOrderByFechaDesc(String clienteOid);
  List<Pedido> findAllByOrderByFechaDesc();

  /** Lee el pedido con SELECT ... FOR UPDATE: otro hilo que lo quiera cambiar espera al commit de este. */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select p from Pedido p where p.id = :id")
  Optional<Pedido> findByIdForUpdate(@Param("id") Long id);
}
