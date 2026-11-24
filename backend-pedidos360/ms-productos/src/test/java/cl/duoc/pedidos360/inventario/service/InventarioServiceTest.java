package cl.duoc.pedidos360.inventario.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.pedidos360.inventario.InventarioProperties;
import cl.duoc.pedidos360.inventario.event.MovimientosInventarioEvent;
import cl.duoc.pedidos360.inventario.repository.EventoProcesadoRepository;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoInventario;
import cl.duoc.pedidos360.kafka.evento.TipoEventoPedido;
import cl.duoc.pedidos360.productos.model.Producto;
import cl.duoc.pedidos360.productos.repository.ProductoRepository;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationEventPublisher;

class InventarioServiceTest {

  private final ProductoRepository productos = mock(ProductoRepository.class);
  private final EventoProcesadoRepository procesados = mock(EventoProcesadoRepository.class);
  private final ApplicationEventPublisher eventos = mock(ApplicationEventPublisher.class);
  private final InventarioService service =
      new InventarioService(productos, procesados, new InventarioProperties(5), eventos);

  private Producto notebook;
  private Producto mouse;

  @BeforeEach
  void setUp() {
    notebook = producto(1L, "Notebook", 10);
    mouse = producto(2L, "Mouse", 3);
    when(productos.findByIdForUpdate(1L)).thenReturn(Optional.of(notebook));
    when(productos.findByIdForUpdate(2L)).thenReturn(Optional.of(mouse));
    when(procesados.saveAndFlush(any())).thenAnswer(inv -> inv.getArgument(0));
  }

  @Test
  void descuentaElStockDeTodosLosProductosYPublicaUnEventoPorProducto() {
    InventarioService.Resultado resultado = service.reservarStock(pedido("e-1", 7L, item(1L, 4), item(2L, 1)));

    assertThat(resultado).isEqualTo(InventarioService.Resultado.DESCONTADO);
    assertThat(notebook.getStock()).isEqualTo(6);
    assertThat(mouse.getStock()).isEqualTo(2);
    List<InventarioEvento> movimientos = publicados();
    assertThat(movimientos).extracting(InventarioEvento::tipo)
        .containsOnly(TipoEventoInventario.STOCK_DESCONTADO);
    assertThat(movimientos).extracting(InventarioEvento::pedidoId).containsOnly(7L);
    // El mouse quedó en 2 (umbral 5): el evento lo marca con stock bajo
    assertThat(movimientos).filteredOn(m -> m.productoId() == 2L).singleElement()
        .satisfies(m -> assertThat(m.stockBajo()).isTrue());
  }

  @Test
  void conStockInsuficienteNoDescuentaNadaYPublicaStockInsuficiente() {
    InventarioService.Resultado resultado = service.reservarStock(pedido("e-2", 8L, item(1L, 2), item(2L, 5)));

    assertThat(resultado).isEqualTo(InventarioService.Resultado.INSUFICIENTE);
    assertThat(notebook.getStock()).isEqualTo(10);
    assertThat(mouse.getStock()).isEqualTo(3);
    assertThat(publicados()).singleElement().satisfies(m -> {
      assertThat(m.tipo()).isEqualTo(TipoEventoInventario.STOCK_INSUFICIENTE);
      assertThat(m.productoId()).isEqualTo(2L);
      assertThat(m.motivo()).contains("disponible 3, solicitado 5");
    });
  }

  @Test
  void unEventoRepetidoNoVuelveADescontar() {
    when(procesados.existsById("e-3")).thenReturn(true);

    InventarioService.Resultado resultado = service.reservarStock(pedido("e-3", 9L, item(1L, 4)));

    assertThat(resultado).isEqualTo(InventarioService.Resultado.DUPLICADO);
    assertThat(notebook.getStock()).isEqualTo(10);
    verify(procesados, never()).saveAndFlush(any());
    verify(eventos, never()).publishEvent(any());
  }

  @Test
  void sumaLasCantidadesDelMismoProductoEnUnPedido() {
    service.reservarStock(pedido("e-4", 10L, item(1L, 3), item(1L, 2)));

    assertThat(notebook.getStock()).isEqualTo(5);
    assertThat(publicados()).singleElement().satisfies(m -> assertThat(m.cantidad()).isEqualTo(5));
  }

  private List<InventarioEvento> publicados() {
    ArgumentCaptor<MovimientosInventarioEvent> captor = ArgumentCaptor.forClass(MovimientosInventarioEvent.class);
    verify(eventos).publishEvent(captor.capture());
    return captor.getValue().movimientos();
  }

  private static Producto producto(Long id, String nombre, int stock) {
    Producto p = new Producto();
    p.setId(id);
    p.setNombre(nombre);
    p.setPrecio(BigDecimal.valueOf(1000));
    p.setStock(stock);
    return p;
  }

  private static PedidoEvento.Item item(Long productoId, int cantidad) {
    return new PedidoEvento.Item(productoId, "Producto " + productoId, cantidad, BigDecimal.TEN,
        BigDecimal.TEN.multiply(BigDecimal.valueOf(cantidad)));
  }

  private static PedidoEvento pedido(String eventoId, Long pedidoId, PedidoEvento.Item... items) {
    return new PedidoEvento(eventoId, TipoEventoPedido.PEDIDO_CREADO, 1, java.time.Instant.now(), pedidoId,
        "cliente-1", "CREADO", BigDecimal.valueOf(100), null, List.of(items));
  }
}
