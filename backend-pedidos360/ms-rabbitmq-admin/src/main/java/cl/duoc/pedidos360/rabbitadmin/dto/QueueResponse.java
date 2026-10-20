package cl.duoc.pedidos360.rabbitadmin.dto;

/** Estado de una cola en el broker. */
public record QueueResponse(String nombre, int mensajes, int consumidores) {}
