package cl.duoc.pedidos360.analitica.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import cl.duoc.pedidos360.analitica.service.AnaliticaService.Origen;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoPedido;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Idempotencia de la analítica (sin Kafka ni contexto Spring). */
class AnaliticaServiceTest {

  private final AnaliticaService analitica = new AnaliticaService();
  private final Origen origen = new Origen("pedidos360.pedidos", 0, 1L);

  @Test
  void unEventoRepetidoNoSumaDosVeces() {
    PedidoEvento e = pedido("evento-1", List.of(item()));

    analitica.registrarPedido(e, origen);
    analitica.registrarPedido(e, origen);

    assertThat(analitica.resumen().totalPedidos()).isEqualTo(1);
    assertThat(analitica.resumen().eventosProcesados()).isEqualTo(1);
  }

  @Test
  void unEventoQueFallaAMitadDeCaminoNoQuedaMarcadoComoProcesado() {
    // Sin ítems, la descripción del evento falla después de haber empezado a procesarlo
    assertThatThrownBy(() -> analitica.registrarPedido(pedido("evento-2", null), origen))
        .isInstanceOf(NullPointerException.class);

    // El reintento del mismo evento (ahora completo) se procesa: no se descarta como repetido
    analitica.registrarPedido(pedido("evento-2", List.of(item())), origen);

    assertThat(analitica.resumen().eventosProcesados()).isEqualTo(1);
    assertThat(analitica.resumen().ultimosEventos()).extracting(r -> r.eventoId()).containsExactly("evento-2");
  }

  private static PedidoEvento.Item item() {
    return new PedidoEvento.Item(3L, "Mouse", 1, BigDecimal.TEN, BigDecimal.TEN);
  }

  private static PedidoEvento pedido(String eventoId, List<PedidoEvento.Item> items) {
    return new PedidoEvento(eventoId, TipoEventoPedido.PEDIDO_CREADO, 1, Instant.now(), 9L, "cliente-1", "CREADO",
        BigDecimal.TEN, null, items);
  }
}
