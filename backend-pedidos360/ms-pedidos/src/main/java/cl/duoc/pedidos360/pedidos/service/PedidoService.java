package cl.duoc.pedidos360.pedidos.service;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.pedidos.client.ProductoDto;
import cl.duoc.pedidos360.pedidos.client.ProductosClient;
import cl.duoc.pedidos360.pedidos.dto.CrearPedidoRequest;
import cl.duoc.pedidos360.pedidos.dto.PedidoResponse;
import cl.duoc.pedidos360.pedidos.model.Pedido;
import cl.duoc.pedidos360.pedidos.model.PedidoItem;
import cl.duoc.pedidos360.pedidos.repository.PedidoRepository;
import cl.duoc.pedidos360.security.JwtUtils;
import cl.duoc.pedidos360.security.SecurityProperties;
import java.math.BigDecimal;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class PedidoService {

  private static final Logger log = LoggerFactory.getLogger(PedidoService.class);

  private final PedidoRepository repository;
  private final ProductosClient productosClient;
  private final SecurityProperties securityProps;

  public PedidoService(PedidoRepository repository, ProductosClient productosClient, SecurityProperties securityProps) {
    this.repository = repository;
    this.productosClient = productosClient;
    this.securityProps = securityProps;
  }

  /**
   * Crea un pedido para el usuario del token: valida cada producto contra ms-productos,
   * calcula el total con el precio vigente.
   */
  public PedidoResponse crear(Jwt jwt, CrearPedidoRequest req) {
    Pedido pedido = new Pedido();
    pedido.setClienteOid(JwtUtils.userId(jwt));
    pedido.setClienteEmail(JwtUtils.email(jwt));
    pedido.setClienteNombre(JwtUtils.name(jwt));
    pedido.setObservacion(req.observacion());

    BigDecimal total = BigDecimal.ZERO;
    for (CrearPedidoRequest.ItemRequest item : req.items()) {
      ProductoDto producto = productosClient.obtener(item.productoId());
      if (producto == null || !Boolean.TRUE.equals(producto.activo())) {
        throw new BusinessException("El producto " + item.productoId() + " no está disponible");
      }
      if (producto.stock() < item.cantidad()) {
        throw new BusinessException("Stock insuficiente para '" + producto.nombre() + "': disponible "
            + producto.stock() + ", solicitado " + item.cantidad());
      }
      PedidoItem pi = new PedidoItem();
      pi.setProductoId(producto.id());
      pi.setNombreProducto(producto.nombre());
      pi.setCantidad(item.cantidad());
      pi.setPrecioUnitario(producto.precio());
      pi.setSubtotal(producto.precio().multiply(BigDecimal.valueOf(item.cantidad())));
      pedido.agregarItem(pi);
      total = total.add(pi.getSubtotal());
    }
    pedido.setTotal(total);

    Pedido guardado = repository.save(pedido);
    log.info("Pedido {} creado por {} ({} ítems, total {})", guardado.getId(), pedido.getClienteEmail(),
        pedido.getItems().size(), total);
    return PedidoResponse.de(guardado);
  }

  /** Admin ve todos los pedidos; un cliente solo los suyos (filtrado por oid del token). */
  @Transactional(readOnly = true)
  public List<PedidoResponse> listar(Jwt jwt) {
    List<Pedido> pedidos = esAdmin(jwt)
        ? repository.findAllByOrderByFechaDesc()
        : repository.findByClienteOidOrderByFechaDesc(JwtUtils.userId(jwt));
    return pedidos.stream().map(PedidoResponse::de).toList();
  }

  @Transactional(readOnly = true)
  public PedidoResponse obtener(Jwt jwt, Long id) {
    Pedido pedido = buscar(id);
    if (!esAdmin(jwt) && !pedido.getClienteOid().equals(JwtUtils.userId(jwt))) {
      throw new AccessDeniedException("El pedido " + id + " pertenece a otro cliente");
    }
    return PedidoResponse.de(pedido);
  }

  private boolean esAdmin(Jwt jwt) {
    return JwtUtils.hasRole(jwt, securityProps.rolesClaim(), "Admin");
  }

  private Pedido buscar(Long id) {
    return repository.findById(id).orElseThrow(() -> new ResourceNotFoundException("Pedido " + id + " no existe"));
  }
}
