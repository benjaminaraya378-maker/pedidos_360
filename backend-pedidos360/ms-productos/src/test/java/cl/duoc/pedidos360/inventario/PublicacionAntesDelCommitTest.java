package cl.duoc.pedidos360.inventario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.pedidos360.inventario.consumer.PedidoEventoConsumer;
import cl.duoc.pedidos360.inventario.producer.InventarioKafkaProducer;
import cl.duoc.pedidos360.inventario.repository.EventoProcesadoRepository;
import cl.duoc.pedidos360.inventario.service.InventarioService;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties;
import cl.duoc.pedidos360.kafka.PublicacionKafkaException;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoPedido;
import cl.duoc.pedidos360.productos.dto.ProductoRequest;
import cl.duoc.pedidos360.productos.model.Producto;
import cl.duoc.pedidos360.productos.repository.ProductoRepository;
import cl.duoc.pedidos360.productos.service.ProductoService;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * Demuestra, con transacciones y eventos de Spring reales (sin base de datos ni Kafka), que publicar antes del commit
 * hace lo que se espera: si Kafka no confirma un evento, la transacción se deshace en vez de confirmarse y la
 * excepción le llega a quien llamó (el listener de PEDIDO_CREADO o el controlador HTTP).
 *
 * <p>El gestor de transacciones de la prueba anota cada paso en {@code pasos}; el KafkaTemplate simulado también, así
 * se ve el orden: inicio → envío a Kafka → commit o rollback.
 */
class PublicacionAntesDelCommitTest {

  private final List<String> pasos = new ArrayList<>();
  private final ProductoRepository productos = mock(ProductoRepository.class);
  private final EventoProcesadoRepository procesados = mock(EventoProcesadoRepository.class);
  @SuppressWarnings("unchecked")
  private final KafkaTemplate<String, InventarioEvento> kafka = mock(KafkaTemplate.class);

  private AnnotationConfigApplicationContext contexto;
  private Producto notebook;

  @BeforeEach
  void setUp() {
    notebook = new Producto();
    notebook.setId(1L);
    notebook.setNombre("Notebook");
    notebook.setPrecio(BigDecimal.valueOf(1000));
    notebook.setStock(10);
    when(productos.findByIdForUpdate(1L)).thenReturn(Optional.of(notebook));
    when(productos.save(any())).thenAnswer(inv -> {
      Producto p = inv.getArgument(0);
      p.setId(25L);
      return p;
    });
    when(procesados.saveAndFlush(any())).thenAnswer(inv -> inv.getArgument(0));

    contexto = new AnnotationConfigApplicationContext();
    contexto.register(ConTransacciones.class);
    contexto.registerBean(GestorQueAnota.class, () -> new GestorQueAnota(pasos));
    contexto.registerBean(ProductoRepository.class, () -> productos);
    contexto.registerBean(EventoProcesadoRepository.class, () -> procesados);
    contexto.registerBean(InventarioProperties.class, () -> new InventarioProperties(5));
    contexto.registerBean(InventarioKafkaProducer.class, () -> new InventarioKafkaProducer(kafka, propiedades(), 200));
    contexto.registerBean(InventarioService.class);
    contexto.registerBean(ProductoService.class);
    contexto.registerBean(PedidoEventoConsumer.class);
    contexto.refresh();
  }

  @AfterEach
  void tearDown() {
    contexto.close();
  }

  @Test
  void conKafkaDisponiblePublicaAntesDelCommitYConfirma() {
    kafkaConfirma();

    InventarioService.Resultado resultado = contexto.getBean(InventarioService.class)
        .reservarStock(pedidoCreado("e-1", 7L));

    assertThat(resultado).isEqualTo(InventarioService.Resultado.DESCONTADO);
    assertThat(pasos).containsExactly("inicio", "kafka STOCK_DESCONTADO", "commit");
  }

  @Test
  void siKafkaFallaElListenerDePedidoCreadoRecibeElErrorYLaTransaccionSeDeshace() {
    kafkaFalla();
    PedidoEventoConsumer consumidor = contexto.getBean(PedidoEventoConsumer.class);

    // La excepción sale del listener: el DefaultErrorHandler no confirma el offset, reintenta y luego va al DLT
    assertThatThrownBy(() -> consumidor.onPedidoEvento(pedidoCreado("e-2", 8L), 0, 12L))
        .isInstanceOf(PublicacionKafkaException.class)
        .hasMessageContaining("STOCK_DESCONTADO del producto 1");

    // Se alcanzó a registrar el evento procesado y a descontar, pero nunca hubo commit: todo eso se deshace
    verify(procesados).saveAndFlush(any());
    assertThat(pasos).containsExactly("inicio", "kafka STOCK_DESCONTADO", "rollback");
  }

  @Test
  void elReintentoDespuesDelRollbackVuelveADescontarEnVezDeMarcarDuplicado() {
    kafkaFalla();
    PedidoEventoConsumer consumidor = contexto.getBean(PedidoEventoConsumer.class);
    assertThatThrownBy(() -> consumidor.onPedidoEvento(pedidoCreado("e-3", 9L), 0, 13L))
        .isInstanceOf(PublicacionKafkaException.class);

    // Tras el rollback el EventoProcesado no existe en la base (el simulado responde false): Kafka ya responde
    kafkaConfirma();
    consumidor.onPedidoEvento(pedidoCreado("e-3", 9L), 0, 13L);

    assertThat(pasos).containsExactly("inicio", "kafka STOCK_DESCONTADO", "rollback",
        "inicio", "kafka STOCK_DESCONTADO", "commit");
  }

  @Test
  void siKafkaFallaAlCrearUnProductoLaExcepcionSaleDelServicioYNoSeConfirma() {
    kafkaFalla();
    ProductoService servicio = contexto.getBean(ProductoService.class);

    assertThatThrownBy(() -> servicio.crear(
        new ProductoRequest("Monitor", null, "Pantallas", BigDecimal.valueOf(150000), 4)))
        .isInstanceOf(PublicacionKafkaException.class)
        .hasMessageContaining("PRODUCTO_CREADO del producto 25");
    assertThat(pasos).containsExactly("inicio", "kafka PRODUCTO_CREADO", "rollback");
  }

  @Test
  void siKafkaFallaAlReponerStockLaExcepcionSaleDelServicioYNoSeConfirma() {
    kafkaFalla();

    assertThatThrownBy(() -> contexto.getBean(InventarioService.class).reponer(1L, 5))
        .isInstanceOf(PublicacionKafkaException.class)
        .hasMessageContaining("STOCK_REPUESTO del producto 1");
    assertThat(pasos).containsExactly("inicio", "kafka STOCK_REPUESTO", "rollback");
  }

  private void kafkaConfirma() {
    doAnswer(inv -> {
      InventarioEvento evento = inv.getArgument(2);
      pasos.add("kafka " + evento.tipo());
      RecordMetadata metadata = new RecordMetadata(new TopicPartition(inv.getArgument(0), 0), 5L, 0, 0L, 0, 0);
      return CompletableFuture.completedFuture(
          new SendResult<>(new ProducerRecord<>(inv.<String>getArgument(0), inv.<String>getArgument(1), evento),
              metadata));
    }).when(kafka).send(anyString(), anyString(), any(InventarioEvento.class));
  }

  /** El broker no confirma: lo mismo que pasa con los brokers caídos al vencer delivery.timeout.ms. */
  private void kafkaFalla() {
    doAnswer(inv -> {
      pasos.add("kafka " + inv.<InventarioEvento>getArgument(2).tipo());
      return CompletableFuture.failedFuture(new org.apache.kafka.common.errors.TimeoutException(
          "Expiring 1 record(s) for pedidos360.inventario-0: 12000 ms has passed since batch creation"));
    }).when(kafka).send(anyString(), anyString(), any(InventarioEvento.class));
  }

  private static PedidoEvento pedidoCreado(String eventoId, Long pedidoId) {
    PedidoEvento.Item item = new PedidoEvento.Item(1L, "Notebook", 2, BigDecimal.TEN, BigDecimal.valueOf(20));
    return new PedidoEvento(eventoId, TipoEventoPedido.PEDIDO_CREADO, 1, Instant.now(), pedidoId, "cliente-1",
        "CREADO", BigDecimal.valueOf(20), null, List.of(item));
  }

  private static KafkaTopicosProperties propiedades() {
    return new KafkaTopicosProperties(
        new KafkaTopicosProperties.Topicos(
            new KafkaTopicosProperties.Topico("pedidos360.pedidos", 3, (short) 3, null, null, 2, null, null),
            new KafkaTopicosProperties.Topico("pedidos360.inventario", 3, (short) 3, "compact", null, 2, null, null)),
        new KafkaTopicosProperties.Dlt(".dlt", 1209600000L),
        new KafkaTopicosProperties.Grupos("pedidos360-inventario"),
        new KafkaTopicosProperties.Reintentos(3, 1000, 2.0, 10000));
  }

  /** Activa @Transactional y @TransactionalEventListener, igual que en la aplicación. */
  @Configuration
  @EnableTransactionManagement
  static class ConTransacciones {}

  /** Gestor de transacciones sin base de datos que anota cada inicio, commit y rollback. */
  static class GestorQueAnota extends AbstractPlatformTransactionManager {

    private final List<String> pasos;

    GestorQueAnota(List<String> pasos) {
      this.pasos = pasos;
    }

    @Override
    protected Object doGetTransaction() {
      return new Object();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
      pasos.add("inicio");
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {
      pasos.add("commit");
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {
      pasos.add("rollback");
    }
  }
}
