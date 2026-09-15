package cl.duoc.pedidos360.bff.dto;

import java.util.List;
import java.util.Map;

public record DashboardResponse(
    MeResponse usuario,
    long productosDisponibles,
    int misPedidos,
    List<Map<String, Object>> ultimosPedidos) {}
