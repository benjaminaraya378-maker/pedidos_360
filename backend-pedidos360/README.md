# Pedidos360 · Backend (Spring Boot 3.5 · Java 21)

Seis microservicios Spring Boot. Cada uno **valida el JWT del IDaaS** (firma vía JWKS, vigencia, issuer, audience)
con Spring Security OAuth2 Resource Server y autoriza por **rol** (claim `roles` → `ROLE_*`) y **scope**
(claim `scp` → `SCOPE_*`). Se despliegan en **EC2** y persisten en una base de datos cloud (**RDS MySQL**).

| Servicio | Puerto | Rutas | Autorización | BD |
|---|---|---|---|---|
| `bff-pedidos360` | 8080 | `GET /api/bff/me` · `GET /api/bff/dashboard` · `GET /api/bff/admin/pedidos` | JWT válido · scope Read · rol Admin | — (compone ms-productos y ms-pedidos reenviando el JWT) |
| `ms-productos` | 8081 | `GET /api/productos` · `GET /api/productos/{id}` · `POST /api/productos` | scope Read · scope Read · rol Admin | `pedidos360_productos` (entidad `Producto`) |
| `ms-pedidos` | 8082 | `POST /api/pedidos` · `GET /api/pedidos` · `GET /api/pedidos/{id}` | scope Write · scope Read (Cliente ve solo los suyos) · dueño o Admin | `pedidos360_pedidos` (entidades `Pedido`, `PedidoItem`) |
| `ms-rabbitmq-admin` | 8083 | `/api/rabbit/queues` · `/api/rabbit/exchanges` · `/api/rabbit/bindings` (ver más abajo) | rol Admin | — (administra RabbitMQ) |
| `ms-kafka-admin` | 8084 | `/api/kafka/cluster` · `/api/kafka/topics` · `/api/kafka/consumer-groups` (ver más abajo) | rol Admin | — (administra Kafka) |
| `ms-analitica` | 8085 | `GET /api/analitica/resumen` · `GET /api/analitica/stock` | rol Admin | — (métricas en memoria, reconstruidas desde Kafka) |

`ms-productos` suma `PATCH /api/productos/{id}/stock` (rol Admin, reponer stock).

Códigos de respuesta: `401` sin token o token inválido (firma, exp, iss, aud) · `403` sin el rol/scope o pedido de
otro usuario · `404` no existe · `400` validación · `409` producto no disponible.

Paquetes comunes (`cl.duoc.pedidos360.security`, `cl.duoc.pedidos360.common`): `SecurityConfig` (`NimbusJwtDecoder` +
`JwtTimestampValidator` + `JwtIssuerValidator` + `AudienceValidator`), `ClaimsAuthoritiesConverter`,
`JsonAuthenticationEntryPoint` (401), `JsonAccessDeniedHandler` (403), `GlobalExceptionHandler`.

## Configuración (variables de entorno o `application.yml`)

| Variable | Valor |
|---|---|
| `JWT_ISSUER` | claim `iss` del token: `https://<tenant-id>.ciamlogin.com/<tenant-id>/v2.0` |
| `JWT_JWKS_URI` | `https://<subdominio>.ciamlogin.com/<tenant-id>/discovery/v2.0/keys` |
| `JWT_AUDIENCE` | client ID de la app **Pedidos360-API** |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:4200,https://<url-del-frontend>` |
| `DB_URL`, `DB_USER`, `DB_PASSWORD` | conexión a RDS MySQL (una por microservicio; ver `application.yml`) |
| `PRODUCTOS_URL`, `PEDIDOS_URL` | URLs internas entre servicios (en EC2: `http://localhost:8081` / `8082`) |
| `RABBITMQ_ADDRESSES` | nodos del clúster RabbitMQ (`ms-pedidos` y `ms-rabbitmq-admin`): `localhost:5672,localhost:5673`. Sin definir, se usa un solo broker (`RABBITMQ_HOST`:`RABBITMQ_PORT`) |
| `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST` | un solo broker y credenciales (por defecto `localhost:5672`, `guest`) |
| `RABBITMQ_MANAGEMENT_URL` | API de gestión de RabbitMQ, solo para listar colas (`ms-rabbitmq-admin`): `http://localhost:15672` (nodo 1) o `:15673` (nodo 2) |
| `COMPROBANTES_DIR` | carpeta donde `ms-pedidos` escribe los tickets (por defecto la carpeta temporal del sistema) |
| `KAFKA_BOOTSTRAP_SERVERS` | brokers Kafka (`ms-pedidos`, `ms-productos`, `ms-analitica`, `ms-kafka-admin`): `localhost:9092,localhost:9093,localhost:9094` (valor por defecto) |
| `UMBRAL_STOCK_BAJO` | `ms-productos`: con este stock o menos el evento de inventario marca `stockBajo` (por defecto 5) |
| `ANALITICA_RECONSTRUIR` | `ms-analitica`: `true` (defecto) relee los tópicos desde el inicio al arrancar para reconstruir sus métricas |
| `KAFKA_UI_USER`, `KAFKA_UI_PASSWORD` | usuario y clave de Kafka UI (por defecto `admin` / `pedidos360`; cambiarlos en la EC2) |
| `KAFKA_HOST_EXTERNO` | host que los brokers anuncian a los clientes fuera de Docker (por defecto `localhost`) |

## Mensajería con RabbitMQ

`ms-pedidos` publica un evento cuando se crea un pedido y lo procesan, de forma asíncrona, tres consumidores. La creación
del pedido no depende del broker: el evento se publica después del commit y, si RabbitMQ no responde, el pedido igual
se crea y el fallo queda en el log.

Todos los nombres están en `application.yml` (`pedidos360.messaging`) y los beans `Queue`, `Exchange` y `Binding` se
declaran en `messaging/RabbitMQConfig`.

| Caso de uso | Exchange → cola (binding) | DLQ | Consumidor |
|---|---|---|---|
| Notificar al cliente | `pedidos360.pedidos.exchange` (topic) → `pedidos360.notificaciones.queue` (`pedido.creado`) | `pedidos360.notificaciones.dlq` | `notificaciones/consumer/NotificacionPedidoConsumer` |
| Generar el comprobante (ticket) | `pedidos360.pedidos.exchange` (topic) → `pedidos360.comprobantes.queue` (`pedido.creado`) | `pedidos360.comprobantes.dlq` | `comprobantes/consumer/ComprobantePedidoConsumer` |
| Registrar la auditoría | `pedidos360.pedidos.exchange` (topic) → `pedidos360.auditoria.queue` (`pedido.creado`) | `pedidos360.auditoria.dlq` | `auditoria/consumer/AuditoriaPedidoConsumer` |

Cada cola principal tiene `x-dead-letter-exchange = pedidos360.pedidos.dlx` (direct), que enruta los rechazados a su DLQ.
Los consumidores usan ACK manual y la misma política de error (`messaging/ConsumerErrorHandler`):

| Situación | Acción |
|---|---|
| Procesado correctamente | `basicAck` |
| JSON ilegible o datos inválidos (p. ej. pedido sin correo) | `basicNack` sin requeue → DLQ, y se registra en el log |
| Error recuperable (p. ej. no se pudo escribir el ticket) | se reintenta hasta `max-reintentos` (3) con el header `x-reintentos`, esperando `retry-delay-ms` x N (1 s, 2 s, 3 s) antes del reintento N |
| Reintentos agotados | `basicNack` sin requeue → DLQ, y se registra en el log |
| No se pudo publicar el reintento | `basicNack` con requeue (el mensaje no se pierde) |

### Clúster RabbitMQ de dos nodos (docker-compose)

`docker-compose.yml` levanta **`rabbitmq1`** (AMQP `5672`, consola `15672`) y **`rabbitmq2`** (AMQP `5673`, consola `15673`) con
RabbitMQ 3.13. Comparten la cookie de Erlang y `rabbitmq/rabbitmq.conf` (peer discovery `classic_config`), y el nodo 2 espera
a que el nodo 1 esté `healthy`. Los puertos se publican solo en `127.0.0.1`; la consola se abre por túnel SSH.

```bash
docker compose up -d
docker compose ps                                  # rabbitmq1 y rabbitmq2 en estado healthy
docker exec rabbitmq1 rabbitmqctl cluster_status   # debe listar rabbit@rabbitmq1 y rabbit@rabbitmq2
export RABBITMQ_ADDRESSES=localhost:5672,localhost:5673   # y reiniciar ms-pedidos y ms-rabbitmq-admin
bash rabbitmq/ha-policy.sh                         # opcional: replica las colas pedidos360.* en los dos nodos
bash rabbitmq/verificar-ep4.sh                     # comprueba clúster, exchanges, colas, bindings y el flujo de un pedido
```

Las colas son clásicas: cada una vive en el nodo donde se declaró, salvo que se aplique `ha-policy.sh`. Con dos nodos no hay
quórum ante una partición de red.

### API del administrador de RabbitMQ (`ms-rabbitmq-admin`, rol Admin)

| Método | Ruta | Cuerpo / parámetros | Respuestas |
|---|---|---|---|
| POST | `/api/rabbit/queues` | `{nombre, durable?, autoDelete?, deadLetterExchange?, deadLetterRoutingKey?, ttlMs?, maxLength?}` | 201 · 400 · 409 |
| GET | `/api/rabbit/queues` | — | 200 `[{nombre, mensajes, consumidores}]` ordenado por nombre · 503 |
| GET | `/api/rabbit/queues/{nombre}` | — | 200 `{nombre, mensajes, consumidores}` · 400 · 404 |
| DELETE | `/api/rabbit/queues/{nombre}` | — | 204 · 400 · 404 |
| DELETE | `/api/rabbit/queues/{nombre}/messages` | — (vacía la cola, la deja y conserva sus bindings) | 200 `{nombre, mensajesEliminados}` · 400 · 404 |
| POST | `/api/rabbit/exchanges` | `{nombre, tipo: direct\|topic\|fanout\|headers, durable?, autoDelete?}` | 201 · 400 · 409 |
| DELETE | `/api/rabbit/exchanges/{nombre}` | — | 204 · 404 |
| POST | `/api/rabbit/bindings` | `{exchange, cola, routingKey?}` | 201 · 400 · 404 |
| DELETE | `/api/rabbit/bindings` | `?exchange=..&cola=..&routingKey=..` | 204 · 400 · 404 |

Validaciones: nombres obligatorios, de máximo 255 caracteres, solo letras, números y `. _ - :` y sin el prefijo
reservado `amq.`; `ttlMs` y `maxLength` mayores que 0; `deadLetterRoutingKey` solo junto con un `deadLetterExchange`
que exista. Los errores vuelven en JSON con el detalle por campo; si el broker no está disponible, 503.
La lógica está en `rabbitadmin/service/RabbitAdminService`; el controlador solo llama a sus métodos. El listado de colas lo
pide a la API de gestión de RabbitMQ (`rabbitadmin/client/RabbitManagementClient`), porque AMQP no sabe listar; si esa API no
responde, el listado devuelve 503 y el resto de las operaciones siguen funcionando.

## Streaming con Kafka (EP5)

Kafka complementa a RabbitMQ sin tocarlo: RabbitMQ sigue repartiendo **tareas** (notificación, comprobante y auditoría,
cada una con su cola y su DLQ); Kafka lleva el **flujo de eventos** que otros servicios leen a su ritmo, pueden releer y
que alimenta la analítica. `ms-pedidos` publica en los dos: el productor de RabbitMQ (`PedidoEventProducer`) y el de
Kafka (`PedidoKafkaProducer`) escuchan el mismo evento de Spring y no se conocen.

### Arquitectura (docker-compose)

| Archivo | Servicios | Puertos (solo `127.0.0.1`) |
|---|---|---|
| `docker-compose.kafka.yml` | `zookeeper1..3` (ensamble con quórum de 2) y `kafka1..3` (brokers 1, 2 y 3) | brokers en `9092`, `9093`, `9094` |
| `docker-compose.kafka-ui.yml` | `kafka-ui` (Provectus Kafka UI, con usuario y clave) | `8090` |

Cada broker tiene dos listeners: `INTERNO` (`kafkaN:29092`, entre brokers y para Kafka UI) y `EXTERNO` (`localhost:909N`,
para los microservicios). Valores del clúster: factor de replicación 3, `min.insync.replicas=2`, sin elección de líderes
atrasados y sin creación automática de tópicos.

```bash
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml ps          # 6 servicios healthy
docker compose -f docker-compose.kafka-ui.yml up -d    # http://localhost:8090
bash kafka/kafka-admin.sh cluster                       # quórum de Zookeeper, brokers y controlador
bash kafka/verificar-ep5.sh                             # comprueba tópicos, réplicas, ISR, configuración y grupos
```

### Tópicos (3 particiones y 3 réplicas cada uno)

| Tópico | Clave | Eventos | Dueño (lo crea) | Configuración y por qué |
|---|---|---|---|---|
| `pedidos360.pedidos` | id del pedido | `PEDIDO_CREADO`, `PEDIDO_CONFIRMADO`, `PEDIDO_CANCELADO` | `ms-pedidos` | `cleanup.policy=delete`, `retention.ms` 7 días: es un historial; 7 días permiten reprocesar o reconstruir la analítica |
| `pedidos360.inventario` | id del producto | `PRODUCTO_CREADO`, `STOCK_DESCONTADO`, `STOCK_INSUFICIENTE`, `STOCK_REPUESTO` | `ms-productos` | `cleanup.policy=compact`, `min.compaction.lag.ms` 7 días, `segment.ms` 1 día: cada evento trae el stock vigente; compactar deja para siempre el último de cada producto |
| `pedidos360.pedidos.dlt`, `pedidos360.inventario.dlt` | la original | eventos que un consumidor no pudo procesar | el dueño del tópico | `delete`, 14 días para revisarlos |

Todos con `min.insync.replicas=2`: con `acks=all`, un evento confirmado está escrito en al menos 2 brokers, y se puede
seguir escribiendo con un broker caído. La clave por pedido (o por producto) hace que sus eventos caigan en la misma
partición y se consuman en orden. Los parámetros viven en `application.yml` (`pedidos360.kafka.topicos`) y `KafkaConfig`
los declara como `NewTopic`; `kafka/kafka-admin.sh crear-topicos` crea lo mismo desde la consola.

### Flujo de eventos y consumer groups

```
POST /api/pedidos ─► ms-pedidos ──PEDIDO_CREADO──► pedidos360.pedidos ──► [pedidos360-inventario] ms-productos
                                                          │                  descuenta stock (todo o nada)
                                                          │                              │
                                                          │        STOCK_DESCONTADO / STOCK_INSUFICIENTE
                                                          │                              ▼
                  ms-pedidos [pedidos360-estado-pedidos] ◄──────────────── pedidos360.inventario
                  CREADO → EN_PREPARACION | CANCELADO                                    │
                  y publica PEDIDO_CONFIRMADO / PEDIDO_CANCELADO                         │
                                                          │                              │
                                                          ▼                              ▼
                                         ms-analitica [pedidos360-analitica] lee los dos tópicos
```

| Grupo | Servicio | Tópicos | Para qué |
|---|---|---|---|
| `pedidos360-inventario` | `ms-productos` (`inventario/consumer/PedidoEventoConsumer`) | pedidos | Descuenta el stock de cada `PEDIDO_CREADO` |
| `pedidos360-estado-pedidos` | `ms-pedidos` (`pedidos/consumer/InventarioEventoConsumer`) | inventario | Confirma o cancela el pedido según el resultado |
| `pedidos360-analitica` | `ms-analitica` (`analitica/consumer/AnaliticaConsumer`) | pedidos e inventario | Métricas, alertas de stock bajo y últimos eventos |

Cada grupo tiene su propio offset: los tres reciben todos los eventos que les interesan. Dentro de un grupo,
`concurrency: 3` pone 3 hilos (uno por partición); si se levanta otra instancia del mismo servicio, Kafka reparte las
particiones entre ambas (`CooperativeStickyAssignor`: los demás consumidores siguen trabajando durante el rebalanceo).

### Contratos (DTO) y serialización

`PedidoEvento` e `InventarioEvento` (paquete `kafka/evento`, un record por servicio, sin librería compartida) llevan
`eventoId` (para idempotencia), `tipo`, `version` del contrato y `fecha`. El productor los serializa con `JsonSerializer`
y pone el tipo lógico en el header `__TypeId__` (`pedidoEvento`, `inventarioEvento`); cada consumidor lo traduce a su
propia clase (`spring.json.type.mapping`). Un tipo de evento nuevo se lee como `DESCONOCIDO` y se ignora.

### Manejo de errores

`KafkaConfig.kafkaErrorHandler` (`DefaultErrorHandler`) en los tres servicios consumidores:

| Situación | Qué pasa |
|---|---|
| Procesado | El contenedor confirma el offset (`ack-mode: record`) |
| JSON ilegible | `ErrorHandlingDeserializer` lo atrapa: va al DLT sin reintentos, con los bytes originales |
| Payload que no cumple el contrato (`@Valid`: id negativo, sin ítems...) | Al DLT sin reintentos |
| Evento imposible de procesar (`EventoInvalidoException`, p. ej. pedido inexistente) | Al DLT sin reintentos |
| Error recuperable (p. ej. base de datos) | Se reintenta con espera exponencial 1 s, 2 s, 4 s; agotados, al DLT |

Cada reintento y cada envío al DLT quedan en el log con tópico, partición, offset, clave y causa. En el DLT, Kafka agrega
headers con el tópico, partición, offset y grupo de origen y la excepción. Un evento malo nunca detiene la partición.
Además, `ms-productos` registra cada evento procesado (`eventos_procesados`) en la misma transacción que el descuento, así
una entrega repetida no descuenta dos veces; y `ms-pedidos` solo cambia pedidos en CREADO, bloqueándolos al leerlos.

### API del administrador de Kafka (`ms-kafka-admin`, rol Admin)

| Método | Ruta | Cuerpo / parámetros | Respuestas |
|---|---|---|---|
| GET | `/api/kafka/cluster` | — | 200 `{clusterId, controlador, brokers}` · 503 |
| GET | `/api/kafka/topics` | `?internos=false` | 200 `[{nombre, particiones, replicas, interno}]` · 503 |
| GET | `/api/kafka/topics/{nombre}` | — | 200 particiones (líder, réplicas, ISR, offsets), mensajes y configuración · 400 · 404 |
| POST | `/api/kafka/topics` | `{nombre, particiones, replicas, configuracion: {"retention.ms": "..."}}` | 201 · 400 · 409 |
| PATCH | `/api/kafka/topics/{nombre}/config` | `{establecer: {...}, restablecer: [...]}` | 200 · 400 · 404 |
| POST | `/api/kafka/topics/{nombre}/partitions` | `{total}` (solo aumentar) | 200 · 400 · 404 |
| DELETE | `/api/kafka/topics/{nombre}` | — (los 4 tópicos del sistema están protegidos) | 204 · 404 · 409 |
| GET | `/api/kafka/consumer-groups` | — | 200 `[{grupo, estado, miembros, lagTotal}]` · 503 |
| GET | `/api/kafka/consumer-groups/{grupo}` | — | 200 miembros, particiones asignadas, offset confirmado, offset final y lag · 404 |
| POST | `/api/kafka/consumer-groups/{grupo}/offsets` | `{topico, estrategia: INICIO\|FIN\|FECHA\|OFFSET, fecha?, offset?, particion?}` | 200 · 400 · 404 · 409 (grupo activo) |
| DELETE | `/api/kafka/consumer-groups/{grupo}` | — | 204 · 404 · 409 (grupo activo) |

Parámetros de tópico permitidos (los demás se rechazan con 400): `cleanup.policy`, `retention.ms`, `retention.bytes`,
`min.insync.replicas` (no mayor que las réplicas), `segment.ms`, `segment.bytes`, `max.message.bytes`, `compression.type`,
`min.compaction.lag.ms`, `delete.retention.ms`. Nombres de tópico: letras, números y `. _ -`, máximo 249, sin `__`.
Toda la interacción con Kafka está en `kafkaadmin/service/KafkaAdminService` (`AdminClient`); Kafka sin respuesta → 503.
Lo mismo por consola, replicable en local y en la EC2: `bash kafka/kafka-admin.sh` (`crear-topicos`, `describir`,
`grupos`, `grupo`, `retencion`, `particiones`, `reset`, `mensajes`, `tolerancia`).

### Analítica (`ms-analitica`, rol Admin)

`GET /api/analitica/resumen` entrega pedidos por estado, pedidos de la última hora, ventas confirmadas, ticket promedio,
tasa de cancelación, más vendidos, alertas de stock bajo, los últimos 15 eventos (con tópico, partición, offset y clave) y
la latencia media entre que ocurre un evento y la analítica lo procesa. Las métricas viven en memoria; al arrancar, el
consumidor vuelve al inicio de cada partición y las reconstruye con lo que Kafka retiene (7 días de pedidos y el último
stock de cada producto). Procesar es idempotente (por `eventoId` y reemplazando estado), así releer no altera los números.

### Pruebas

`mvn test` en cada servicio: `ms-pedidos` (`EstadoPedidoServiceTest`), `ms-productos` (`InventarioServiceTest`),
`ms-kafka-admin` (`ConfiguracionesTopicoTest`) y `ms-analitica` (`AnaliticaKafkaTest`, con un broker Kafka embebido:
eventos de los dos tópicos, JSON ilegible y payload inválido al DLT, y la partición sigue después del error).

## Ejecutar / desplegar en EC2

```bash
# Amazon Linux 2023: sudo dnf install -y java-21-amazon-corretto maven git docker
sudo docker rm -f rabbitmq 2>/dev/null; sudo docker compose up -d   # clúster RabbitMQ de 2 nodos (ver arriba)
sudo docker compose -f docker-compose.kafka.yml up -d               # 3 Zookeeper + 3 brokers Kafka
sudo docker compose -f docker-compose.kafka-ui.yml up -d            # Kafka UI (túnel SSH al 8090)
export RABBITMQ_ADDRESSES=localhost:5672,localhost:5673
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094
mvn -f ms-productos/pom.xml      -DskipTests package && nohup java -jar ms-productos/target/ms-productos-1.0.0.jar &
mvn -f ms-pedidos/pom.xml        -DskipTests package && nohup java -jar ms-pedidos/target/ms-pedidos-1.0.0.jar &
mvn -f bff-pedidos360/pom.xml    -DskipTests package && nohup java -jar bff-pedidos360/target/bff-pedidos360-1.0.0.jar &
mvn -f ms-rabbitmq-admin/pom.xml -DskipTests package && nohup java -jar ms-rabbitmq-admin/target/ms-rabbitmq-admin-1.0.0.jar &
mvn -f ms-kafka-admin/pom.xml    -DskipTests package && nohup java -jar ms-kafka-admin/target/ms-kafka-admin-1.0.0.jar &
mvn -f ms-analitica/pom.xml      -DskipTests package && nohup java -jar ms-analitica/target/ms-analitica-1.0.0.jar &
```
Exportar antes las variables de la tabla. Security group de la EC2: puertos 8080-8085 abiertos para el API Gateway
(los de Kafka, Zookeeper y Kafka UI no se abren: se publican solo en `127.0.0.1`).
La consola de RabbitMQ (`:15672` y `:15673`) muestra los nodos, colas y DLQ; no se abre al público.

## API Gateway (HTTP API) — rutas, authorizer y CORS

Integraciones HTTP hacia `http://<ip-ec2>:8080|8081|8082|8083|8084|8085` y rutas explícitas:

| Método | Ruta | Backend | Scope en el authorizer |
|---|---|---|---|
| GET | `/api/bff/me` | 8080 | (solo token válido) |
| GET | `/api/bff/dashboard` | 8080 | `Pedidos360.Read` |
| GET | `/api/bff/admin/pedidos` | 8080 | `Pedidos360.Read` (el rol Admin lo valida el BFF) |
| GET / POST | `/api/productos` | 8081 | `Pedidos360.Read` / `Pedidos360.Write` |
| GET | `/api/productos/{id}` | 8081 | `Pedidos360.Read` |
| PATCH | `/api/productos/{id}/stock` | 8081 | `Pedidos360.Write` (el rol Admin lo valida el servicio) |
| GET / POST | `/api/pedidos` | 8082 | `Pedidos360.Read` / `Pedidos360.Write` |
| GET | `/api/pedidos/{id}` | 8082 | `Pedidos360.Read` |
| ANY | `/api/rabbit/{proxy+}` | 8083 | (el rol Admin lo valida el servicio) |
| ANY | `/api/kafka/{proxy+}` | 8084 | (el rol Admin lo valida el servicio) |
| GET | `/api/analitica/{proxy+}` | 8085 | `Pedidos360.Read` (el rol Admin lo valida el servicio) |

- **Authorizer JWT**: identity source `$request.header.Authorization`, issuer = `JWT_ISSUER`, audience = `JWT_AUDIENCE`;
  adjunto a todas las rutas. Sin token / token inválido → 401; sin el scope → 403.
- **CORS**: orígenes `https://<url-del-frontend>` y `http://localhost:4200`; métodos `GET, POST, PATCH, DELETE, OPTIONS`;
  headers `Authorization, Content-Type`.
