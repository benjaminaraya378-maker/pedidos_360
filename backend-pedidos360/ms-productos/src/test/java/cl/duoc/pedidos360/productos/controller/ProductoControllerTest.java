package cl.duoc.pedidos360.productos.controller;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import cl.duoc.pedidos360.common.GlobalExceptionHandler;
import cl.duoc.pedidos360.inventario.service.InventarioService;
import cl.duoc.pedidos360.kafka.PublicacionKafkaException;
import cl.duoc.pedidos360.productos.service.ProductoService;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** Si Kafka no confirma el evento de inventario, las rutas que escriben responden 503 con el JSON de error común. */
class ProductoControllerTest {

  private final ProductoService productoService = mock(ProductoService.class);
  private final InventarioService inventarioService = mock(InventarioService.class);
  private final MockMvc mvc = MockMvcBuilders
      .standaloneSetup(new ProductoController(productoService, inventarioService))
      .setControllerAdvice(new GlobalExceptionHandler())
      .build();

  @Test
  void reponerStockConKafkaCaidoResponde503() throws Exception {
    when(inventarioService.reponer(3L, 10)).thenThrow(kafkaNoConfirmo("STOCK_REPUESTO", 3L));

    mvc.perform(patch("/api/productos/3/stock").contentType(MediaType.APPLICATION_JSON).content("{\"cantidad\":10}"))
        .andExpect(status().isServiceUnavailable())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status").value(503))
        .andExpect(jsonPath("$.error").value("Service Unavailable"))
        .andExpect(jsonPath("$.path").value("/api/productos/3/stock"))
        .andExpect(jsonPath("$.message").value(containsString("STOCK_REPUESTO del producto 3")))
        .andExpect(jsonPath("$.message").value(containsString("No se guardó ningún cambio")))
        .andExpect(jsonPath("$.timestamp").exists())
        .andExpect(jsonPath("$.details").isArray());
  }

  @Test
  void crearProductoConKafkaCaidoResponde503() throws Exception {
    when(productoService.crear(any())).thenThrow(kafkaNoConfirmo("PRODUCTO_CREADO", 25L));

    mvc.perform(post("/api/productos").contentType(MediaType.APPLICATION_JSON)
            .content("{\"nombre\":\"Monitor\",\"precio\":150000,\"stock\":4}"))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.status").value(503))
        .andExpect(jsonPath("$.path").value("/api/productos"))
        .andExpect(jsonPath("$.message").value(containsString("PRODUCTO_CREADO")));
  }

  private static PublicacionKafkaException kafkaNoConfirmo(String tipo, Long productoId) {
    return new PublicacionKafkaException("Kafka no confirmó el evento " + tipo + " del producto " + productoId
        + " (Expiring 1 record(s) for pedidos360.inventario-0: 12000 ms has passed since batch creation)",
        new org.apache.kafka.common.errors.TimeoutException("Expiring 1 record(s)"));
  }
}
