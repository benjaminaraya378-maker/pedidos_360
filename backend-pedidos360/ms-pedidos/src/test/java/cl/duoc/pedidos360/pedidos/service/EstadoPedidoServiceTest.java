package cl.duoc.pedidos360.pedidos.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.pedidos360.kafka.EventoInvalidoException;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoInventario;
import cl.duoc.pedidos360.pedidos.event.PedidoEstadoCambiadoEvent;
import cl.duoc.pedidos360.pedidos.model.EstadoPedido;
import cl.duoc.pedidos360.pedidos.model.Pedido;
import cl.duoc.pedidos360.pedidos.repository.PedidoRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationEventPublisher;

class EstadoPedidoServiceTest {

  private final PedidoRepository repository = mock(PedidoRepository.class);
  private final ApplicationEventPublisher eventos = mock(ApplicationEventPublisher.class);
  private final EstadoPedidoService service = new EstadoPedidoService(repository, eventos);

  private Pedido pedido;

  @BeforeEach
  void setUp() {
    pedido = new Pedido();
    pedido.setId(15L);
    pedido.setClienteOid("cliente-1");
    pedido.setTotal(BigDecimal.valueOf(25990));
    when(repository.findByIdForUpdate(15L)).thenReturn(Optional.of(pedido));
  }

  @Test
  void stockDescontadoPasaElPedidoAEnPreparacionYLoAnuncia() {
    service.aplicarResultadoInventario(evento(TipoEventoInventario.STOCK_DESCONTADO, 15L, null));

    assertThat(pedido.getEstado()).isEqualTo(EstadoPedido.EN_PREPARACION);
    ArgumentCaptor<PedidoEstadoCambiadoEvent> captor = ArgumentCaptor.forClass(PedidoEstadoCambiadoEvent.class);
    verify(eventos).publishEvent(captor.capture());
    assertThat(captor.getValue().estado()).isEqualTo(EstadoPedido.EN_PREPARACION);
  }

  @Test
  void stockInsuficienteCancelaElPedidoConElMotivo() {
    service.aplicarResultadoInventario(evento(TipoEventoInventario.STOCK_INSUFICIENTE, 15L,
        "Stock insuficiente de 'Mouse': disponible 1, solicitado 3"));

    assertThat(pedido.getEstado()).isEqualTo(EstadoPedido.CANCELADO);
    assertThat(pedido.getMotivoEstado()).contains("disponible 1, solicitado 3");
  }

  @Test
  void unSegundoEventoDelMismoPedidoNoLoVuelveACambiar() {
    pedido.setEstado(EstadoPedido.EN_PREPARACION);

    service.aplicarResultadoInventario(evento(TipoEventoInventario.STOCK_DESCONTADO, 15L, null));

    assertThat(pedido.getEstado()).isEqualTo(EstadoPedido.EN_PREPARACION);
    verify(eventos, never()).publishEvent(any());
  }

  @Test
  void losEventosSinPedidoNoAfectanPedidos() {
    service.aplicarResultadoInventario(evento(TipoEventoInventario.STOCK_REPUESTO, null, null));

    verify(repository, never()).findByIdForUpdate(any());
    verify(eventos, never()).publishEvent(any());
  }

  @Test
  void unPedidoInexistenteEsUnEventoInvalido() {
    when(repository.findByIdForUpdate(99L)).thenReturn(Optional.empty());

    assertThatThrownBy(() -> service.aplicarResultadoInventario(
        evento(TipoEventoInventario.STOCK_DESCONTADO, 99L, null)))
        .isInstanceOf(EventoInvalidoException.class)
        .hasMessageContaining("El pedido 99");
  }

  private static InventarioEvento evento(TipoEventoInventario tipo, Long pedidoId, String motivo) {
    return new InventarioEvento("ev-" + tipo, tipo, 1, Instant.now(), 3L, "Mouse", 2, 5, 3, 5, true, pedidoId,
        motivo);
  }
}
