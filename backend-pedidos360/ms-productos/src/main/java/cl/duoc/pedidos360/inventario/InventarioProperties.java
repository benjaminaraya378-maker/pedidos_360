package cl.duoc.pedidos360.inventario;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** @param umbralStockBajo con este stock o menos, el producto se marca con stock bajo en sus eventos */
@ConfigurationProperties(prefix = "pedidos360.inventario")
public record InventarioProperties(int umbralStockBajo) {}
