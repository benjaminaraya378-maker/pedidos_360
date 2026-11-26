package cl.duoc.pedidos360.kafkaadmin.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import cl.duoc.pedidos360.common.GlobalExceptionHandler;
import cl.duoc.pedidos360.common.KafkaNoDisponibleException;
import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.common.ValidacionException;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.Reinyeccion;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.ResultadoReproceso;
import cl.duoc.pedidos360.kafkaadmin.service.DltService;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaAdminService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** Rutas de DLT: validación de la entrada y errores con el JSON común (400, 404, 503). */
class KafkaAdminControllerDltTest {

  private static final String DLT = "pedidos360.pedidos.dlt";

  private final DltService dlt = mock(DltService.class);
  private final MockMvc mvc = MockMvcBuilders
      .standaloneSetup(new KafkaAdminController(mock(KafkaAdminService.class), dlt))
      .setControllerAdvice(new GlobalExceptionHandler())
      .build();

  @Test
  void reprocesarDevuelveLoReinyectado() throws Exception {
    when(dlt.reprocesar(DLT, 5)).thenReturn(new ResultadoReproceso(DLT, "ms-kafka-admin-reproceso", 1, 0,
        List.of(new Reinyeccion(0, 3L, "P-1", "pedidos360.pedidos", 2, 57L))));

    mvc.perform(post("/api/kafka/dlt/" + DLT + "/reprocesar").contentType(MediaType.APPLICATION_JSON)
            .content("{\"max\":5}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.reinyectados").value(1))
        .andExpect(jsonPath("$.detalle[0].topico").value("pedidos360.pedidos"))
        .andExpect(jsonPath("$.detalle[0].offset").value(57));
  }

  @Test
  void maxFueraDeRangoOAusenteResponde400SinLlamarAlServicio() throws Exception {
    mvc.perform(post("/api/kafka/dlt/" + DLT + "/reprocesar").contentType(MediaType.APPLICATION_JSON)
            .content("{\"max\":101}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.details", hasItem(containsString("máximo 100"))));
    mvc.perform(post("/api/kafka/dlt/" + DLT + "/reprocesar").contentType(MediaType.APPLICATION_JSON)
            .content("{}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.details", hasItem(containsString("es obligatorio"))));
    mvc.perform(get("/api/kafka/dlt/" + DLT).param("max", "0"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.details", hasItem(containsString("mínimo 1"))));

    verify(dlt, never()).reprocesar(anyString(), anyInt());
    verify(dlt, never()).listar(anyString(), anyInt());
  }

  @Test
  void topicoQueNoEsDltResponde400() throws Exception {
    when(dlt.reprocesar("pedidos360.pedidos", 10)).thenThrow(new ValidacionException("Tópico DLT inválido",
        List.of("topicoDlt: solo se pueden leer y reprocesar DLT: el nombre debe terminar en '.dlt'")));

    mvc.perform(post("/api/kafka/dlt/pedidos360.pedidos/reprocesar").contentType(MediaType.APPLICATION_JSON)
            .content("{\"max\":10}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.details[0]").value(containsString("debe terminar en '.dlt'")));
  }

  @Test
  void dltInexistenteResponde404YKafkaCaidoResponde503() throws Exception {
    when(dlt.listar("pedidos360.pagos.dlt", 20))
        .thenThrow(new ResourceNotFoundException("No se pudo describir el tópico 'pedidos360.pagos.dlt': no existe"));
    when(dlt.listar(DLT, 20)).thenThrow(new KafkaNoDisponibleException("Kafka no respondió en 10 s"));

    mvc.perform(get("/api/kafka/dlt/pedidos360.pagos.dlt"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.path").value("/api/kafka/dlt/pedidos360.pagos.dlt"));
    mvc.perform(get("/api/kafka/dlt/" + DLT))   // max por defecto: 20
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.status").value(503));
  }

  /** Errores del propio Spring MVC: conservan su código (antes el manejador genérico los convertía en 500). */
  @Test
  void textoDondeVaUnNumeroResponde400YMetodoNoPermitidoResponde405() throws Exception {
    mvc.perform(get("/api/kafka/dlt/" + DLT).param("max", "veinte"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value(containsString("'max' debe ser de tipo")));
    mvc.perform(delete("/api/kafka/dlt/" + DLT))
        .andExpect(status().isMethodNotAllowed())
        .andExpect(jsonPath("$.message").value("Método DELETE no permitido en /api/kafka/dlt/" + DLT));

    verify(dlt, never()).listar(anyString(), anyInt());
  }
}
