package cl.duoc.pedidos360.rabbitadmin.dto;

/**
 * Regla única para nombres de colas y exchanges: no vacíos, máximo 255 caracteres, solo caracteres seguros
 * y sin el prefijo {@code amq.} (reservado por RabbitMQ para sus exchanges internos).
 */
public final class ReglasNombre {

  /** El vacío lo rechaza {@code @NotBlank}; este patrón cubre caracteres, largo y prefijo reservado. */
  public static final String PATRON = "^(?!amq\\.)[A-Za-z0-9._:-]{0,255}$";
  public static final String MENSAJE =
      "solo puede tener letras, números y . _ - : (máximo 255) y no puede empezar con 'amq.'";
  public static final String OBLIGATORIO = "es obligatorio";

  private ReglasNombre() {}
}
