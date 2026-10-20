package cl.duoc.pedidos360.rabbitadmin.dto;

/** Exchange creado. */
public record ExchangeResponse(String nombre, String tipo, boolean durable, boolean autoDelete) {}
