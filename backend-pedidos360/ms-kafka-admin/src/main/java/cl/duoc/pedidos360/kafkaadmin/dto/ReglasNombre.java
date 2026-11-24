package cl.duoc.pedidos360.kafkaadmin.dto;

/**
 * Reglas de nombres de Kafka: tópicos de 1 a 249 caracteres con letras, números, punto, guion bajo y guion (las mismas
 * que acepta el broker), sin el prefijo {@code __} de los tópicos internos (por ejemplo {@code __consumer_offsets}).
 */
public final class ReglasNombre {

  public static final String TOPICO = "^(?!__)[A-Za-z0-9._-]{1,249}$";
  public static final String MENSAJE_TOPICO =
      "solo puede tener letras, números y . _ - (máximo 249) y no puede empezar con '__'";

  public static final String GRUPO = "^[A-Za-z0-9._-]{1,255}$";
  public static final String MENSAJE_GRUPO = "solo puede tener letras, números y . _ - (máximo 255)";

  public static final String OBLIGATORIO = "es obligatorio";

  private ReglasNombre() {}
}
