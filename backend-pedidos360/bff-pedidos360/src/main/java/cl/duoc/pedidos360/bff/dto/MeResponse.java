package cl.duoc.pedidos360.bff.dto;

import java.util.List;

/** Identidad del usuario según el token validado por el BFF. */
public record MeResponse(
    String nombre,
    String email,
    String oid,
    List<String> roles,
    List<String> scopes,
    String issuer,
    List<String> audience,
    String expira,
    String validadoPor) {}
