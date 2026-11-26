package cl.duoc.pedidos360.rabbitadmin.dto;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Pruebas unitarias (sin contexto Spring ni RabbitMQ) de las reglas con que ms-rabbitmq-admin valida una cola antes de
 * crearla: nombre, prefijo reservado {@code amq.}, largo y coherencia del dead letter exchange.
 */
class QueueRequestTest {

  private static jakarta.validation.ValidatorFactory fabrica;
  private static Validator validador;

  @BeforeAll
  static void iniciar() {
    fabrica = Validation.buildDefaultValidatorFactory();
    validador = fabrica.getValidator();
  }

  @AfterAll
  static void cerrar() {
    fabrica.close();
  }

  private static QueueRequest cola(String nombre) {
    return new QueueRequest(nombre, null, null, null, null, null, null);
  }

  @Test
  void unaColaValidaPasaYTomaLosValoresPorDefecto() {
    QueueRequest req = new QueueRequest("pedidos360.notificaciones", null, null, "pedidos360.dlx", "notificaciones",
        60000, 1000);

    assertThat(validador.validate(req)).isEmpty();
    assertThat(req.durable()).isTrue();
    assertThat(req.autoDelete()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"amq.interna", "con espacio", "acento-ñ", "barra/no"})
  void rechazaNombresReservadosOConCaracteresNoPermitidos(String nombre) {
    Set<ConstraintViolation<QueueRequest>> errores = validador.validate(cola(nombre));

    assertThat(errores).extracting(v -> v.getPropertyPath().toString()).containsExactly("nombre");
  }

  @Test
  void rechazaUnNombreDeMasDe255Caracteres() {
    assertThat(validador.validate(cola("a".repeat(256)))).hasSize(1);
    assertThat(validador.validate(cola("a".repeat(255)))).isEmpty();
  }

  @Test
  void rechazaUnNombreVacio() {
    assertThat(validador.validate(cola(""))).extracting(ConstraintViolation::getMessage)
        .containsExactly(ReglasNombre.OBLIGATORIO);
    // Solo espacios: es obligatorio y, además, el espacio no es un carácter permitido
    assertThat(validador.validate(cola("  "))).extracting(ConstraintViolation::getMessage)
        .contains(ReglasNombre.OBLIGATORIO);
  }

  @Test
  void laRoutingKeyDeDeadLetterExigeUnExchange() {
    QueueRequest req = new QueueRequest("pedidos360.auditoria", true, false, null, "auditoria", null, null);

    assertThat(validador.validate(req)).extracting(v -> v.getPropertyPath().toString())
        .containsExactly("deadLetterConsistente");
  }

  @Test
  void ttlYLargoMaximoDebenSerPositivos() {
    QueueRequest req = new QueueRequest("pedidos360.auditoria", true, false, null, null, 0, -1);

    assertThat(validador.validate(req)).hasSize(2);
  }
}
