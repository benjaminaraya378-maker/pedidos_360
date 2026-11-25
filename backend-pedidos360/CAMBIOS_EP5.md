# EP5 · Cambios sobre EP4 (streaming y analítica con Kafka)

EP5 pide integrar Kafka sin afectar lo que ya hace RabbitMQ. Nada de la mensajería de EP3/EP4 cambió: las 3 colas, sus DLQ,
los consumidores y `ms-rabbitmq-admin` siguen igual. Kafka se suma con un caso de uso que el sistema no tenía: **el stock
nunca se descontaba** (ms-pedidos solo lo validaba). Ahora cada pedido viaja por Kafka al inventario, que descuenta el
stock y responde; el pedido se confirma o se cancela solo, y una analítica en tiempo real lee todo el flujo.

## Dónde se cumple cada indicador de la pauta

| Indicador | Peso | Dónde |
|---|---|---|
| Configuración completa del cliente Kafka (bootstrap servers, serializers, consumer groups, propiedades avanzadas) | 12 % | `spring.kafka` en el `application.yml` de `ms-pedidos`, `ms-productos` y `ms-analitica` (brokers del compose, `acks=all`, idempotencia, `ErrorHandlingDeserializer` + `JsonDeserializer`, `enable-auto-commit: false`, `CooperativeStickyAssignor`, `ack-mode: record`, `concurrency: 3`) y `kafka/KafkaConfig` (serializador por tipo, manejo de errores, validación) |
| Productores y consumidores con DTO sobre los dos tópicos | 12 % | `kafka/evento/PedidoEvento` e `InventarioEvento` (records con Bean Validation); `ms-pedidos` escribe en pedidos y lee inventario; `ms-productos` lee pedidos y escribe inventario; `ms-analitica` lee los dos |
| Microservicio administrador: crear, listar y mantener tópicos y configuraciones | 6 % | `ms-kafka-admin`: `KafkaAdminController` → `KafkaAdminService` (`AdminClient`) |
| Productores con DTO y serializers adecuados | 14 % | `ms-pedidos/.../pedidos/producer/PedidoKafkaProducer`, `ms-productos/.../inventario/producer/InventarioKafkaProducer`; `JsonSerializer` con tipo lógico en `__TypeId__` y `ByteArraySerializer` para reenviar al DLT mensajes ilegibles (`DelegatingByTypeSerializer`) |
| Consumidores con `@KafkaListener`, lógica separada de la mensajería | 14 % | `ms-productos/.../inventario/consumer/PedidoEventoConsumer` → `InventarioService`; `ms-pedidos/.../pedidos/consumer/InventarioEventoConsumer` → `EstadoPedidoService`; `ms-analitica/.../analitica/consumer/AnaliticaConsumer` (`@KafkaHandler` por tipo) → `AnaliticaService` |
| Manejo de errores: logging, validación de payloads, control de reintentos | 12 % | `KafkaConfig.kafkaErrorHandler` (`DefaultErrorHandler` + `ExponentialBackOffWithMaxRetries` 1 s/2 s/4 s + `DeadLetterPublishingRecoverer` a `*.dlt`; no recuperables sin reintentos: JSON ilegible, `@Valid`, `EventoInvalidoException`); idempotencia (`eventos_procesados`, bloqueo de filas) |
| Parámetros de administración de tópicos ajustados a cada flujo | 12 % | `pedidos360.kafka.topicos` en `application.yml` + `KafkaConfig.topico()`: pedidos `delete` 7 días; inventario `compact` (clave = producto) con `min.compaction.lag.ms` 7 días y `segment.ms` 1 día; DLT `delete` 14 días; todos 3 particiones, 3 réplicas, `min.insync.replicas=2` |
| Consumer groups para balancear carga y repartir responsabilidades | 12 % | `pedidos360.kafka.grupos`: `pedidos360-inventario`, `pedidos360-estado-pedidos`, `pedidos360-analitica`; 3 hilos por grupo (uno por partición) |
| Operaciones replicables para crear, actualizar e inspeccionar tópicos y grupos | 6 % | `ms-kafka-admin` (`/api/kafka/topics`, `/config`, `/partitions`, `/consumer-groups`, `/offsets`) y `kafka/kafka-admin.sh` (CLI oficial dentro del broker) |

Instrucciones específicas: `docker-compose.kafka.yml` (3 Zookeeper + 3 brokers), `docker-compose.kafka-ui.yml` (Kafka UI),
`docker-compose.yml` (RabbitMQ de EP4, sin cambios), `.gitignore` de cada repositorio (sin `target/`, `node_modules/`, `.env`).

## Código nuevo o modificado

| Archivo | Cambio |
|---|---|
| `ms-pedidos/.../kafka/*` | `KafkaConfig`, `KafkaTopicosProperties`, `EventoInvalidoException` y los DTO de eventos |
| `ms-pedidos/.../pedidos/producer/PedidoKafkaProducer` | publica `PEDIDO_CREADO` (después del commit) y `PEDIDO_CONFIRMADO` / `PEDIDO_CANCELADO` |
| `ms-pedidos/.../pedidos/consumer/InventarioEventoConsumer` + `service/EstadoPedidoService` | confirma o cancela el pedido según el inventario (idempotente, con bloqueo de fila) |
| `ms-pedidos/.../pedidos/model/Pedido`, `dto/PedidoResponse` | campo `motivoEstado` |
| `ms-pedidos/.../pedidos/repository/PedidoRepository` | `findByIdForUpdate` (SELECT ... FOR UPDATE) |
| `ms-productos/.../kafka/*` | igual que en ms-pedidos; aquí `KafkaConfig` declara el tópico de inventario y su DLT |
| `ms-productos/.../inventario/**` | consumidor de pedidos, `InventarioService` (descuento todo o nada, reposición), productor, `EventoProcesado` |
| `ms-productos/.../productos/controller/ProductoController` | `PATCH /api/productos/{id}/stock` (rol Admin) |
| `ms-productos/.../productos/service/ProductoService` | anuncia el alta de un producto en Kafka |
| `ms-analitica/**` (nuevo, puerto 8085) | consumidor de los dos tópicos y `GET /api/analitica/resumen` |
| `ms-kafka-admin/**` (nuevo, puerto 8084) | API de administración de tópicos, configuraciones y consumer groups |
| Pruebas | `EstadoPedidoServiceTest`, `InventarioServiceTest`, `ConfiguracionesTopicoTest`, `AnaliticaKafkaTest` (Kafka embebido) |

## Infraestructura nueva

| Archivo | Para qué |
|---|---|
| `docker-compose.kafka.yml` | `zookeeper1..3` y `kafka1..3` (Confluent Platform 7.6.1), healthchecks y `depends_on: service_healthy`; red `pedidos360-kafka` |
| `docker-compose.kafka-ui.yml` | `kafka-ui` (Provectus 0.7.2) en `127.0.0.1:8090`, con usuario y clave |
| `kafka/kafka-admin.sh` | crear tópicos, describir, grupos y lag, retención, particiones, reset de offsets, mensajes y prueba de tolerancia a fallos |
| `kafka/verificar-ep5.sh` | comprueba quórum, brokers, tópicos (3 × 3, ISR, configuración), DLT, grupos, Kafka UI y, con `GATEWAY` + `TOKEN_CLIENTE`, el cambio de estado de un pedido |
| `kafka/desplegar-ep5-ec2.sh` | despliegue en la EC2: Kafka, Kafka UI, compilación y reinicio de los 4 servicios con las variables de EP1-EP4, y verificación |
| `kafka/rutas-api-gateway-ep5.sh` | agrega las 3 rutas de EP5 al HTTP API con el authorizer JWT existente y suma PATCH al CORS (idempotente; `SIMULAR=1` para revisar) |

## Frontend

Vistas nuevas `/analitica` y `/kafka` (rol Admin), seguimiento automático del estado en `/pedidos` y botón de reposición
en `/productos` (ver `frontend-pedidos360/README.md`). `proxy.conf.json` suma `/api/kafka` (8084) y `/api/analitica` (8085).

## Despliegue en la EC2

Dos scripts lo automatizan (no se pudieron probar contra AWS: el laboratorio no estaba activo; el de rutas se probó en modo `SIMULAR=1`):

```bash
# En la EC2, dentro de backend-pedidos360, después de git pull (lee las variables de EP1-EP4 desde .env):
bash kafka/desplegar-ep5-ec2.sh      # Kafka + Kafka UI, compila y reinicia los 4 servicios, y verifica

# Desde un equipo con AWS CLI y credenciales vigentes del laboratorio:
API_ID=<id del HTTP API> EC2_HOST=<ip de la EC2> bash kafka/rutas-api-gateway-ep5.sh
```

A mano equivale a:

```bash
sudo docker compose -f docker-compose.kafka.yml up -d
sudo docker compose -f docker-compose.kafka-ui.yml up -d
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094
# recompilar y reiniciar ms-pedidos y ms-productos; compilar e iniciar ms-kafka-admin (8084) y ms-analitica (8085)
bash kafka/verificar-ep5.sh
```

Security group: abrir 8084 y 8085 al API Gateway (como 8080-8083); Kafka (9092-9094), Zookeeper y Kafka UI (8090) no se
abren: se publican en `127.0.0.1` y Kafka UI se abre por túnel SSH. API Gateway: `ANY /api/kafka/{proxy+}` → 8084,
`GET /api/analitica/{proxy+}` → 8085, `PATCH /api/productos/{id}/stock` → 8081, con el mismo authorizer JWT, y `PATCH` en
los métodos de CORS.

## Qué se probó (local, Docker Desktop, 24-nov-2026)

Clúster real de `docker-compose.kafka.yml` + Kafka UI + RabbitMQ de EP4 + MySQL 8.4, con `ms-productos`, `ms-pedidos`,
`ms-analitica` y `ms-kafka-admin` desde sus JAR. JWT firmados con una clave RSA local publicada como JWKS (misma estrategia
que en EP4); los servicios validan firma, emisor, audiencia, rol y scope igual que con Entra.

- `mvn test`: 15 pruebas en verde (14 unitarias + `AnaliticaKafkaTest` con Kafka embebido). `ng build` sin errores.
- 6 contenedores `healthy`; Zookeeper con 1 líder y 2 seguidores; brokers 1, 2 y 3 registrados.
- Los servicios dueños crearon los 4 tópicos al iniciar: 3 particiones, 3 réplicas, ISR completo, líderes repartidos en los
  3 brokers y la configuración de cada flujo. `kafka/verificar-ep5.sh`: todo OK (26 comprobaciones).
- Un pedido de 2 notebooks y 1 mouse: `PEDIDO_CREADO` → ms-productos descuenta 20→18 y 8→7 → ms-pedidos pasa el pedido a
  EN_PREPARACION y publica `PEDIDO_CONFIRMADO`, en 0,76 s. En paralelo, RabbitMQ hizo la notificación, el comprobante y la
  auditoría como en EP4 (`rabbitmq/verificar-ep4.sh` sigue todo OK).
- Dos pedidos simultáneos de 2 teclados con stock 3: los dos responden 201; uno queda EN_PREPARACION y el otro CANCELADO
  ("disponible 1, solicitado 2"); el stock queda en 1, nunca negativo.
- JSON ilegible y payload sin ítems con `pedidoId` negativo publicados a mano: al DLT al primer intento, una vez por cada
  grupo que los leyó (`kafka_dlt-original-consumer-group`), con los bytes originales. Un evento de inventario de un pedido
  inexistente: `EventoInvalidoException` → `pedidos360.inventario.dlt`.
- `ms-kafka-admin`: 401 sin token, 403 con rol Cliente, crear (201), repetir (409), validaciones (400 con detalle),
  retención en caliente, 3→6 particiones, reducir (400), borrar tópico protegido (409), borrar (204), 404, grupos con lag,
  mover offsets de un grupo activo (409) y detenido (200), crear y borrar un grupo (200/204/404).
- Con `ms-analitica` detenido y 3 pedidos nuevos, su grupo quedó EMPTY con lag 10; al volver, releyó desde el inicio y quedó
  con lag 0 y las métricas completas.
- Con `kafka3` detenido: el líder de su partición pasó a otro broker, el ISR quedó en 2 y un pedido nuevo se completó igual;
  al levantarlo volvió al ISR.

## Limitaciones conocidas

- Si Kafka está caído justo después de guardar un pedido, el evento no se reenvía después (queda el error en el log) y el
  pedido se queda en CREADO. Igual que con RabbitMQ en EP4, resolverlo requiere un patrón outbox.
- Las métricas de `ms-analitica` viven en memoria: se reconstruyen al arrancar con lo que Kafka retiene (7 días de pedidos,
  último stock de cada producto). Si se escala, cada réplica debe usar su propio grupo para ver todos los eventos.
- Los DLT no tienen un consumidor que reprocese: se revisan en Kafka UI y se pueden reinyectar a mano.
