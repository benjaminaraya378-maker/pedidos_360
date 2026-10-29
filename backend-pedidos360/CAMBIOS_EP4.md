# EP4 · Cambios sobre EP3 (RabbitMQ en clúster de 2 nodos + auditoría)

EP4 pide mostrar dos nodos RabbitMQ en clúster, tres colas con su DLQ, exchanges `topic` y `direct`, y `docker compose`
levantando RabbitMQ en la nube. La lógica de negocio no cambia: el único código nuevo es el tercer caso de uso.

## Código nuevo o modificado

| Archivo | Cambio |
|---|---|
| `ms-pedidos/.../auditoria/consumer/AuditoriaPedidoConsumer` | `@RabbitListener` de la cola de auditoría; ACK manual y errores por `ConsumerErrorHandler` |
| `ms-pedidos/.../auditoria/service/AuditoriaService` | registra en el log una línea `AUDITORIA` por pedido (id, cliente, ítems, total, fecha); un evento sin id es error no recuperable |
| `ms-pedidos/.../messaging/MessagingProperties` | nueva ruta `auditoria` y `retryDelayMs` |
| `ms-pedidos/.../messaging/ConsumerErrorHandler` | espera `retry-delay-ms` x N antes del reintento N (y NACK con requeue si se interrumpe) |
| `ms-pedidos/.../messaging/RabbitMQConfig` | beans de `pedidos360.auditoria.queue`, `pedidos360.auditoria.dlq` y sus 2 bindings; Javadoc con las 3 rutas |
| `ms-pedidos/.../application.yml` | `pedidos360.messaging.auditoria` y `spring.rabbitmq.addresses` (`RABBITMQ_ADDRESSES`) |
| `ms-rabbitmq-admin/.../application.yml` | `spring.rabbitmq.addresses` (`RABBITMQ_ADDRESSES`) y `pedidos360.rabbit-admin.management-url` (`RABBITMQ_MANAGEMENT_URL`) |
| `ms-rabbitmq-admin/.../RabbitAdminController` y `RabbitAdminService` | `GET /api/rabbit/queues` (listar) y `DELETE /api/rabbit/queues/{nombre}/messages` (vaciar) |
| `ms-rabbitmq-admin/.../client/RabbitManagementClient` | pide el listado a la API de gestión de RabbitMQ (AMQP no lista colas); sin respuesta → 503 |
| `ms-rabbitmq-admin/.../dto/PurgaResponse` | `{nombre, mensajesEliminados}` |

`ConsumerErrorHandler` sigue sin lógica por cola (usa la cola del propio mensaje, `getConsumerQueue()`), así que sirve igual
para auditoría. Se le agregó una espera creciente antes de cada reintento (`retry-delay-ms` x N: 1 s, 2 s y 3 s), para que un
error pasajero tenga tiempo de resolverse antes de agotar los 3 reintentos.

## Infraestructura nueva

| Archivo | Para qué |
|---|---|
| `docker-compose.yml` | `rabbitmq1` (5672 / 15672) y `rabbitmq2` (5673 / 15672 del contenedor → 15673) con RabbitMQ 3.13, healthcheck y `depends_on: service_healthy` |
| `rabbitmq/rabbitmq.conf` | formación del clúster (`classic_config`, `rabbit@rabbitmq1` y `rabbit@rabbitmq2`) |
| `rabbitmq/ha-policy.sh` | opcional: política `ha-mode: all` para las colas `pedidos360.*` |
| `rabbitmq/verificar-ep4.sh` | comprueba clúster, exchanges, colas, DLQ, bindings y, con `GATEWAY` + `TOKEN_CLIENTE`, el 401/200 del JWT y las tres líneas de log de un pedido |

## Despliegue en la EC2 (infraestructura de EP1 a EP3)

```bash
docker compose version                       # verificar que Compose esté disponible
sudo docker rm -f rabbitmq                   # reemplaza el docker run anterior
sudo docker compose up -d                    # desde backend-pedidos360
sudo docker compose ps                       # Figura 2: ambos healthy
sudo docker exec rabbitmq1 rabbitmqctl cluster_status   # Figura 3
export RABBITMQ_ADDRESSES=localhost:5672,localhost:5673
# recompilar (mvn -DskipTests package) y reiniciar ms-pedidos y ms-rabbitmq-admin
bash rabbitmq/ha-policy.sh                   # opcional
export GATEWAY=https://<id>.execute-api.<region>.amazonaws.com TOKEN_CLIENTE=<token>
bash rabbitmq/verificar-ep4.sh
```

En el security group se mantienen cerrados 5672, 5673, 15672 y 15673 (el compose los publica solo en `127.0.0.1`; la
consola se abre por túnel SSH). Para el administrador, 8083 abierto al API Gateway y la ruta `ANY /api/rabbit/{proxy+}` con
el mismo authorizer JWT.

## Qué se probó (local, Docker Desktop, 28-oct-2026)

- `mvn package` de `ms-pedidos` y `ms-rabbitmq-admin`: compilan sin errores.
- `docker compose up -d`: `rabbitmq1` y `rabbitmq2` quedan `healthy`; `cluster_status` lista los dos nodos en *Disk Nodes* y
  *Running Nodes*.
- `ms-pedidos` con `RABBITMQ_ADDRESSES=localhost:5672,localhost:5673` crea 2 exchanges (topic y direct), 6 colas y 6 bindings;
  `verificar-ep4.sh` pasa todas las comprobaciones de clúster y topología.
- Un evento `pedido.creado` válido publicado en el exchange deja en el log la notificación, el comprobante y `AUDITORIA`; un
  mensaje ilegible va a las tres DLQ (en `pedidos360.auditoria.dlq` con `x-death`, motivo `rejected`).
- `ha-policy.sh` aplica la política `ha-pedidos360`; `ms-rabbitmq-admin` arranca contra el clúster y responde 401 sin token.
- API del administrador con JWT firmados (JWKS local): 54 casos, entre ellos 401 sin token / vencido / audiencia o emisor
  incorrectos, 403 con rol Cliente, 400 por nombres vacíos, con símbolos, de 300 caracteres o con prefijo `amq.`, 409 al repetir,
  404 al no existir, el listado de colas y el vaciado (3 mensajes publicados, 3 eliminados). Sin la API de gestión, el listado
  responde 503 y `GET /queues/{nombre}` sigue respondiendo 200.
- Error recuperable (directorio de comprobantes inválido): reintentos a +1 s, +2 s y +3 s, luego DLQ; notificación y auditoría
  del mismo pedido se procesan al instante, sin esperar.

## Pendiente (requiere la cuenta AWS y el IDaaS)

Despliegue en la EC2, capturas del informe (Figuras 2 a 7) y la demostración del API Gateway, el JWT, el tenant de Entra y el
login OAuth 2.0 / OpenID Connect. El crear un pedido real a través del Gateway no se probó en local.
