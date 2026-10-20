# Pedidos360 · Backend (Spring Boot 3.5 · Java 21)

Tres microservicios Spring Boot. Cada uno **valida el JWT del IDaaS** (firma vía JWKS, vigencia, issuer, audience)
con Spring Security OAuth2 Resource Server y autoriza por **rol** (claim `roles` → `ROLE_*`) y **scope**
(claim `scp` → `SCOPE_*`). Se despliegan en **EC2** y persisten en una base de datos cloud (**RDS MySQL**).

| Servicio | Puerto | Rutas | Autorización | BD |
|---|---|---|---|---|
| `bff-pedidos360` | 8080 | `GET /api/bff/me` · `GET /api/bff/dashboard` · `GET /api/bff/admin/pedidos` | JWT válido · scope Read · rol Admin | — (compone ms-productos y ms-pedidos reenviando el JWT) |
| `ms-productos` | 8081 | `GET /api/productos` · `GET /api/productos/{id}` · `POST /api/productos` | scope Read · scope Read · rol Admin | `pedidos360_productos` (entidad `Producto`) |
| `ms-pedidos` | 8082 | `POST /api/pedidos` · `GET /api/pedidos` · `GET /api/pedidos/{id}` | scope Write · scope Read (Cliente ve solo los suyos) · dueño o Admin | `pedidos360_pedidos` (entidades `Pedido`, `PedidoItem`) |
| `ms-rabbitmq-admin` | 8083 | `/api/rabbit/queues` · `/api/rabbit/exchanges` · `/api/rabbit/bindings` (ver más abajo) | rol Admin | — (administra RabbitMQ) |

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
| `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST` | broker RabbitMQ (`ms-pedidos` y `ms-rabbitmq-admin`; por defecto `localhost:5672`, `guest`) |
| `COMPROBANTES_DIR` | carpeta donde `ms-pedidos` escribe los tickets (por defecto la carpeta temporal del sistema) |

## Mensajería con RabbitMQ

`ms-pedidos` publica un evento cuando se crea un pedido y lo procesan, de forma asíncrona, dos consumidores. La creación
del pedido no depende del broker: el evento se publica después del commit y, si RabbitMQ no responde, el pedido igual
se crea y el fallo queda en el log.

Todos los nombres están en `application.yml` (`pedidos360.messaging`) y los beans `Queue`, `Exchange` y `Binding` se
declaran en `messaging/RabbitMQConfig`.

| Caso de uso | Exchange → cola (binding) | DLQ | Consumidor |
|---|---|---|---|
| Notificar al cliente | `pedidos360.pedidos.exchange` (topic) → `pedidos360.notificaciones.queue` (`pedido.creado`) | `pedidos360.notificaciones.dlq` | `notificaciones/consumer/NotificacionPedidoConsumer` |
| Generar el comprobante (ticket) | `pedidos360.pedidos.exchange` (topic) → `pedidos360.comprobantes.queue` (`pedido.creado`) | `pedidos360.comprobantes.dlq` | `comprobantes/consumer/ComprobantePedidoConsumer` |

Cada cola principal tiene `x-dead-letter-exchange = pedidos360.pedidos.dlx` (direct), que enruta los rechazados a su DLQ.
Los consumidores usan ACK manual y la misma política de error (`messaging/ConsumerErrorHandler`):

| Situación | Acción |
|---|---|
| Procesado correctamente | `basicAck` |
| JSON ilegible o datos inválidos (p. ej. pedido sin correo) | `basicNack` sin requeue → DLQ, y se registra en el log |
| Error recuperable (p. ej. no se pudo escribir el ticket) | se reintenta hasta `max-reintentos` (3) con el header `x-reintentos` |
| Reintentos agotados | `basicNack` sin requeue → DLQ, y se registra en el log |
| No se pudo publicar el reintento | `basicNack` con requeue (el mensaje no se pierde) |

### API del administrador de RabbitMQ (`ms-rabbitmq-admin`, rol Admin)

| Método | Ruta | Cuerpo / parámetros | Respuestas |
|---|---|---|---|
| POST | `/api/rabbit/queues` | `{nombre, durable?, autoDelete?, deadLetterExchange?, deadLetterRoutingKey?, ttlMs?, maxLength?}` | 201 · 400 · 409 |
| GET | `/api/rabbit/queues/{nombre}` | — | 200 `{nombre, mensajes, consumidores}` · 404 |
| DELETE | `/api/rabbit/queues/{nombre}` | — | 204 · 404 |
| POST | `/api/rabbit/exchanges` | `{nombre, tipo: direct\|topic\|fanout\|headers, durable?, autoDelete?}` | 201 · 400 · 409 |
| DELETE | `/api/rabbit/exchanges/{nombre}` | — | 204 · 404 |
| POST | `/api/rabbit/bindings` | `{exchange, cola, routingKey?}` | 201 · 400 · 404 |
| DELETE | `/api/rabbit/bindings` | `?exchange=..&cola=..&routingKey=..` | 204 · 400 · 404 |

Validaciones: nombres obligatorios, de máximo 255 caracteres, solo letras, números y `. _ - :` y sin el prefijo
reservado `amq.`; `ttlMs` y `maxLength` mayores que 0; `deadLetterRoutingKey` solo junto con un `deadLetterExchange`
que exista. Los errores vuelven en JSON con el detalle por campo; si el broker no está disponible, 503.
La lógica está en `rabbitadmin/service/RabbitAdminService`; el controlador solo llama a sus métodos.

## Ejecutar / desplegar en EC2

```bash
# Amazon Linux 2023: sudo dnf install -y java-21-amazon-corretto maven git docker
sudo docker run -d --name rabbitmq --restart unless-stopped -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management
mvn -f ms-productos/pom.xml      -DskipTests package && nohup java -jar ms-productos/target/ms-productos-1.0.0.jar &
mvn -f ms-pedidos/pom.xml        -DskipTests package && nohup java -jar ms-pedidos/target/ms-pedidos-1.0.0.jar &
mvn -f bff-pedidos360/pom.xml    -DskipTests package && nohup java -jar bff-pedidos360/target/bff-pedidos360-1.0.0.jar &
mvn -f ms-rabbitmq-admin/pom.xml -DskipTests package && nohup java -jar ms-rabbitmq-admin/target/ms-rabbitmq-admin-1.0.0.jar &
```
Exportar antes las variables de la tabla. Security group de la EC2: puertos 8080-8083 abiertos para el API Gateway.
La consola de RabbitMQ (`:15672`) muestra las colas y DLQ; no se abre al público.

## API Gateway (HTTP API) — rutas, authorizer y CORS

Integraciones HTTP hacia `http://<ip-ec2>:8080|8081|8082` y rutas explícitas:

| Método | Ruta | Backend | Scope en el authorizer |
|---|---|---|---|
| GET | `/api/bff/me` | 8080 | (solo token válido) |
| GET | `/api/bff/dashboard` | 8080 | `Pedidos360.Read` |
| GET | `/api/bff/admin/pedidos` | 8080 | `Pedidos360.Read` (el rol Admin lo valida el BFF) |
| GET / POST | `/api/productos` | 8081 | `Pedidos360.Read` / `Pedidos360.Write` |
| GET | `/api/productos/{id}` | 8081 | `Pedidos360.Read` |
| GET / POST | `/api/pedidos` | 8082 | `Pedidos360.Read` / `Pedidos360.Write` |
| GET | `/api/pedidos/{id}` | 8082 | `Pedidos360.Read` |
| POST / GET / DELETE | `/api/rabbit/{proxy+}` | 8083 | (el rol Admin lo valida el servicio) |

- **Authorizer JWT**: identity source `$request.header.Authorization`, issuer = `JWT_ISSUER`, audience = `JWT_AUDIENCE`;
  adjunto a todas las rutas. Sin token / token inválido → 401; sin el scope → 403.
- **CORS**: orígenes `https://<url-del-frontend>` y `http://localhost:4200`; métodos `GET, POST, DELETE, OPTIONS`;
  headers `Authorization, Content-Type`.
