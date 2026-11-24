package cl.duoc.pedidos360.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfiguracionesTopicoTest {

  @Test
  void aceptaLosParametrosDeLosTopicosDelSistema() {
    Map<String, String> pedidos = Map.of(
        "cleanup.policy", "delete",
        "retention.ms", "604800000",
        "min.insync.replicas", "2");
    Map<String, String> inventario = Map.of(
        "cleanup.policy", "compact",
        "min.compaction.lag.ms", "604800000",
        "segment.ms", "86400000",
        "min.insync.replicas", "2");

    assertThat(ConfiguracionesTopico.validar(pedidos, 3)).isEmpty();
    assertThat(ConfiguracionesTopico.validar(inventario, 3)).isEmpty();
  }

  @Test
  void rechazaParametrosNoPermitidosYValoresFueraDeRango() {
    List<String> errores = ConfiguracionesTopico.validar(Map.of(
        "unclean.leader.election.enable", "true",
        "retention.ms", "0",
        "cleanup.policy", "borrar",
        "compression.type", "rar"), 3);

    assertThat(errores).hasSize(4);
    assertThat(errores).anyMatch(e -> e.startsWith("unclean.leader.election.enable: parámetro no permitido"));
    assertThat(errores).anyMatch(e -> e.startsWith("retention.ms: debe ser -1"));
    assertThat(errores).anyMatch(e -> e.startsWith("cleanup.policy:"));
    assertThat(errores).anyMatch(e -> e.startsWith("compression.type:"));
  }

  @Test
  void minInsyncReplicasNoPuedeSuperarLasReplicas() {
    List<String> errores = ConfiguracionesTopico.validar(Map.of("min.insync.replicas", "3"), 2);

    assertThat(errores).singleElement().asString().contains("no puede ser mayor que las réplicas del tópico (2)");
  }

  @Test
  void retencionMenosUnoEsSinLimite() {
    assertThat(ConfiguracionesTopico.validar(Map.of("retention.ms", "-1"), 3)).isEmpty();
    assertThat(ConfiguracionesTopico.validar(Map.of("retention.ms", "una semana"), 3))
        .singleElement().asString().contains("número entero");
  }

  @Test
  void soloSePuedenRestablecerParametrosPermitidos() {
    assertThat(ConfiguracionesTopico.validarClaves(List.of("retention.ms"))).isEmpty();
    assertThat(ConfiguracionesTopico.validarClaves(List.of("leader.replication.throttled.replicas"))).hasSize(1);
  }
}
