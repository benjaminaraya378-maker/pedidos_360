package cl.duoc.pedidos360.kafkaadmin.controller;

import cl.duoc.pedidos360.kafkaadmin.dto.ConfiguracionRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.ParticionesRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.ReglasNombre;
import cl.duoc.pedidos360.kafkaadmin.dto.ReprocesoRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.ResetOffsetsRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.Cluster;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.ContenidoDlt;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.GrupoDetalle;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.GrupoResumen;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.OffsetsMovidos;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.ResultadoReproceso;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.TopicoDetalle;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.TopicoResumen;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicoRequest;
import cl.duoc.pedidos360.kafkaadmin.service.DltService;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaAdminService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * API REST del administrador de Kafka. Todas las rutas exigen JWT válido con rol Admin.
 * <pre>
 *   GET    /api/kafka/cluster                               brokers y controlador              200 · 503
 *   GET    /api/kafka/topics?internos=false                 lista los tópicos                  200 · 503
 *   GET    /api/kafka/topics/{nombre}                       particiones, ISR, offsets, config  200 · 400 · 404
 *   POST   /api/kafka/topics                                crea un tópico                     201 · 400 · 409
 *   PATCH  /api/kafka/topics/{nombre}/config                cambia o restablece parámetros     200 · 400 · 404
 *   POST   /api/kafka/topics/{nombre}/partitions            aumenta las particiones            200 · 400 · 404
 *   DELETE /api/kafka/topics/{nombre}                       elimina el tópico                  204 · 404 · 409
 *   GET    /api/kafka/consumer-groups                       grupos con estado, miembros y lag  200 · 503
 *   GET    /api/kafka/consumer-groups/{grupo}               miembros, asignación, offsets, lag 200 · 400 · 404
 *   POST   /api/kafka/consumer-groups/{grupo}/offsets       mueve offsets (reprocesar/saltar)  200 · 400 · 404 · 409
 *   DELETE /api/kafka/consumer-groups/{grupo}               elimina un grupo sin consumidores  204 · 404 · 409
 *   GET    /api/kafka/dlt/{topicoDlt}?max=20                pendientes del DLT, sin consumir   200 · 400 · 404
 *   POST   /api/kafka/dlt/{topicoDlt}/reprocesar            reinyecta en su tópico de origen   200 · 400 · 404 · 409
 * </pre>
 * Entradas inválidas → 400 con el detalle de cada campo. Kafka sin respuesta → 503.
 */
@RestController
@RequestMapping("/api/kafka")
@PreAuthorize("hasRole('ADMIN')")
public class KafkaAdminController {

  private final KafkaAdminService service;
  private final DltService dlt;

  public KafkaAdminController(KafkaAdminService service, DltService dlt) {
    this.service = service;
    this.dlt = dlt;
  }

  @GetMapping("/cluster")
  public Cluster cluster() {
    return service.cluster();
  }

  // ---------- Tópicos ----------

  @GetMapping("/topics")
  public List<TopicoResumen> listarTopicos(@RequestParam(defaultValue = "false") boolean internos) {
    return service.listarTopicos(internos);
  }

  @GetMapping("/topics/{nombre}")
  public TopicoDetalle obtenerTopico(
      @PathVariable @Pattern(regexp = ReglasNombre.TOPICO, message = ReglasNombre.MENSAJE_TOPICO) String nombre) {
    return service.obtenerTopico(nombre);
  }

  @PostMapping("/topics")
  public ResponseEntity<TopicoDetalle> crearTopico(@Valid @RequestBody TopicoRequest req) {
    TopicoDetalle creado = service.crearTopico(req);
    return ResponseEntity.created(URI.create("/api/kafka/topics/" + creado.nombre())).body(creado);
  }

  @PatchMapping("/topics/{nombre}/config")
  public TopicoDetalle modificarConfiguracion(
      @PathVariable @Pattern(regexp = ReglasNombre.TOPICO, message = ReglasNombre.MENSAJE_TOPICO) String nombre,
      @Valid @RequestBody ConfiguracionRequest req) {
    return service.modificarConfiguracion(nombre, req);
  }

  @PostMapping("/topics/{nombre}/partitions")
  public TopicoDetalle aumentarParticiones(
      @PathVariable @Pattern(regexp = ReglasNombre.TOPICO, message = ReglasNombre.MENSAJE_TOPICO) String nombre,
      @Valid @RequestBody ParticionesRequest req) {
    return service.aumentarParticiones(nombre, req.total());
  }

  @DeleteMapping("/topics/{nombre}")
  public ResponseEntity<Void> eliminarTopico(
      @PathVariable @Pattern(regexp = ReglasNombre.TOPICO, message = ReglasNombre.MENSAJE_TOPICO) String nombre) {
    service.eliminarTopico(nombre);
    return ResponseEntity.noContent().build();
  }

  // ---------- Consumer groups ----------

  @GetMapping("/consumer-groups")
  public List<GrupoResumen> listarGrupos() {
    return service.listarGrupos();
  }

  @GetMapping("/consumer-groups/{grupo}")
  public GrupoDetalle obtenerGrupo(
      @PathVariable @Pattern(regexp = ReglasNombre.GRUPO, message = ReglasNombre.MENSAJE_GRUPO) String grupo) {
    return service.obtenerGrupo(grupo);
  }

  @PostMapping("/consumer-groups/{grupo}/offsets")
  public OffsetsMovidos moverOffsets(
      @PathVariable @Pattern(regexp = ReglasNombre.GRUPO, message = ReglasNombre.MENSAJE_GRUPO) String grupo,
      @Valid @RequestBody ResetOffsetsRequest req) {
    return service.moverOffsets(grupo, req);
  }

  @DeleteMapping("/consumer-groups/{grupo}")
  public ResponseEntity<Void> eliminarGrupo(
      @PathVariable @Pattern(regexp = ReglasNombre.GRUPO, message = ReglasNombre.MENSAJE_GRUPO) String grupo) {
    service.eliminarGrupo(grupo);
    return ResponseEntity.noContent().build();
  }

  // ---------- DLT (dead letter topics) ----------

  /** Lo que reinyectaría el próximo reproceso: posición, origen, excepción y valor de cada mensaje. No consume nada. */
  @GetMapping("/dlt/{topicoDlt}")
  public ContenidoDlt listarDlt(
      @PathVariable @Pattern(regexp = ReglasNombre.TOPICO, message = ReglasNombre.MENSAJE_TOPICO) String topicoDlt,
      @RequestParam(defaultValue = "20") @Min(value = 1, message = "mínimo 1")
      @Max(value = 100, message = "máximo 100") int max) {
    return dlt.listar(topicoDlt, max);
  }

  /** Reinyecta hasta {@code max} mensajes pendientes en su tópico de origen y confirma su offset en el DLT. */
  @PostMapping("/dlt/{topicoDlt}/reprocesar")
  public ResultadoReproceso reprocesarDlt(
      @PathVariable @Pattern(regexp = ReglasNombre.TOPICO, message = ReglasNombre.MENSAJE_TOPICO) String topicoDlt,
      @Valid @RequestBody ReprocesoRequest req) {
    return dlt.reprocesar(topicoDlt, req.max());
  }
}
