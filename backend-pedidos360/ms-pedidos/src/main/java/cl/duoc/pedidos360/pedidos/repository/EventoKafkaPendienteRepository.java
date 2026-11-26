package cl.duoc.pedidos360.pedidos.repository;

import cl.duoc.pedidos360.pedidos.model.EventoKafkaPendiente;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/** Tabla del outbox de Kafka ({@code eventos_kafka_pendientes}). */
public interface EventoKafkaPendienteRepository extends JpaRepository<EventoKafkaPendiente, String> {

  /**
   * Pendientes creados antes de {@code limite}: primero los que llevan menos intentos y, entre ellos, los más
   * antiguos. Así una fila que falla una y otra vez pasa al final y no frena a las que vienen detrás.
   */
  List<EventoKafkaPendiente> findByCreadoEnBeforeOrderByIntentosAscCreadoEnAsc(Instant limite, Limit lote);

  /** Kafka confirmó el evento: sale del outbox. Devuelve 0 si ya no estaba (lo publicó el otro camino). */
  @Transactional
  @Modifying
  @Query("delete from EventoKafkaPendiente e where e.eventoId = :eventoId")
  int borrar(@Param("eventoId") String eventoId);

  /** Kafka no confirmó: la fila sigue pendiente con un intento más y el motivo del fallo. */
  @Transactional
  @Modifying
  @Query("update EventoKafkaPendiente e set e.intentos = e.intentos + 1, e.ultimoError = :error"
      + " where e.eventoId = :eventoId")
  int registrarFallo(@Param("eventoId") String eventoId, @Param("error") String error);
}
