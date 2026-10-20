package cl.duoc.pedidos360.rabbitadmin.controller;

import cl.duoc.pedidos360.rabbitadmin.dto.BindingRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.BindingResponse;
import cl.duoc.pedidos360.rabbitadmin.dto.ExchangeRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.ExchangeResponse;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueResponse;
import cl.duoc.pedidos360.rabbitadmin.dto.ReglasNombre;
import cl.duoc.pedidos360.rabbitadmin.service.RabbitAdminService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * API REST del administrador de RabbitMQ. Todas las rutas exigen JWT válido con rol Admin.
 * <pre>
 *   POST   /api/rabbit/queues                                  crea una cola              201 · 400 · 409
 *   GET    /api/rabbit/queues/{nombre}                         mensajes y consumidores    200 · 404
 *   DELETE /api/rabbit/queues/{nombre}                         elimina la cola            204 · 404
 *   POST   /api/rabbit/exchanges                               crea un exchange           201 · 400 · 409
 *   DELETE /api/rabbit/exchanges/{nombre}                      elimina el exchange        204 · 404
 *   POST   /api/rabbit/bindings                                enlaza cola y exchange     201 · 400 · 404
 *   DELETE /api/rabbit/bindings?exchange=..&cola=..&routingKey=..   elimina el binding    204 · 404
 * </pre>
 * Entradas inválidas → 400 con el detalle de cada campo. Broker caído → 503.
 */
@RestController
@RequestMapping("/api/rabbit")
@PreAuthorize("hasRole('ADMIN')")
public class RabbitAdminController {

  private final RabbitAdminService service;

  public RabbitAdminController(RabbitAdminService service) {
    this.service = service;
  }

  // ---------- Colas ----------

  @PostMapping("/queues")
  public ResponseEntity<QueueResponse> crearCola(@Valid @RequestBody QueueRequest req) {
    QueueResponse creada = service.crearCola(req);
    return ResponseEntity.created(URI.create("/api/rabbit/queues/" + creada.nombre())).body(creada);
  }

  @GetMapping("/queues/{nombre}")
  public QueueResponse obtenerCola(
      @PathVariable @Pattern(regexp = ReglasNombre.PATRON, message = ReglasNombre.MENSAJE) String nombre) {
    return service.obtenerCola(nombre);
  }

  @DeleteMapping("/queues/{nombre}")
  public ResponseEntity<Void> eliminarCola(
      @PathVariable @Pattern(regexp = ReglasNombre.PATRON, message = ReglasNombre.MENSAJE) String nombre) {
    service.eliminarCola(nombre);
    return ResponseEntity.noContent().build();
  }

  // ---------- Exchanges ----------

  @PostMapping("/exchanges")
  public ResponseEntity<ExchangeResponse> crearExchange(@Valid @RequestBody ExchangeRequest req) {
    return ResponseEntity.status(HttpStatus.CREATED).body(service.crearExchange(req));
  }

  @DeleteMapping("/exchanges/{nombre}")
  public ResponseEntity<Void> eliminarExchange(
      @PathVariable @Pattern(regexp = ReglasNombre.PATRON, message = ReglasNombre.MENSAJE) String nombre) {
    service.eliminarExchange(nombre);
    return ResponseEntity.noContent().build();
  }

  // ---------- Bindings ----------

  @PostMapping("/bindings")
  public ResponseEntity<BindingResponse> crearBinding(@Valid @RequestBody BindingRequest req) {
    return ResponseEntity.status(HttpStatus.CREATED).body(service.crearBinding(req));
  }

  /** Los datos del binding van como parámetros de consulta (un DELETE no lleva cuerpo). */
  @DeleteMapping("/bindings")
  public ResponseEntity<Void> eliminarBinding(@Valid BindingRequest req) {
    service.eliminarBinding(req);
    return ResponseEntity.noContent().build();
  }
}
