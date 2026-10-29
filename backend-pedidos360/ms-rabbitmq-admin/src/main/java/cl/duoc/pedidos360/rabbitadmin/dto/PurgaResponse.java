package cl.duoc.pedidos360.rabbitadmin.dto;

/** Resultado de vaciar una cola: cuántos mensajes se eliminaron. */
public record PurgaResponse(String nombre, int mensajesEliminados) {}
