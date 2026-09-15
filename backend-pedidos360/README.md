# Pedidos360 · Backend (Spring Boot 3.5 · Java 21)

Tres microservicios Spring Boot. Cada uno **valida el JWT del IDaaS** (firma vía JWKS, vigencia, issuer, audience)
con Spring Security OAuth2 Resource Server y autoriza por **rol** (claim `roles` → `ROLE_*`) y **scope**
(claim `scp` → `SCOPE_*`). Se despliegan en **EC2** y persisten en una base de datos cloud (**RDS MySQL**).

| Servicio | Puerto | Rutas | Autorización | BD |
|---|---|---|---|---|
| `bff-pedidos360` | 8080 | `GET /api/bff/me` · `GET /api/bff/dashboard` · `GET /api/bff/admin/pedidos` | JWT válido · scope Read · rol Admin | — (compone ms-productos y ms-pedidos reenviando el JWT) |
| `ms-productos` | 8081 | `GET /api/productos` · `GET /api/productos/{id}` · `POST /api/productos` | scope Read · scope Read · rol Admin | `pedidos360_productos` (entidad `Producto`) |
| `ms-pedidos` | 8082 | `POST /api/pedidos` · `GET /api/pedidos` · `GET /api/pedidos/{id}` | scope Write · scope Read (Cliente ve solo los suyos) · dueño o Admin | `pedidos360_pedidos` (entidades `Pedido`, `PedidoItem`) |

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

## Ejecutar / desplegar en EC2

```bash
# Amazon Linux 2023: sudo dnf install -y java-21-amazon-corretto maven git
mvn -f ms-productos/pom.xml   -DskipTests package && nohup java -jar ms-productos/target/ms-productos-1.0.0.jar &
mvn -f ms-pedidos/pom.xml     -DskipTests package && nohup java -jar ms-pedidos/target/ms-pedidos-1.0.0.jar &
mvn -f bff-pedidos360/pom.xml -DskipTests package && nohup java -jar bff-pedidos360/target/bff-pedidos360-1.0.0.jar &
```
Exportar antes las variables de la tabla. Security group de la EC2: puertos 8080-8082 abiertos para el API Gateway.

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

- **Authorizer JWT**: identity source `$request.header.Authorization`, issuer = `JWT_ISSUER`, audience = `JWT_AUDIENCE`;
  adjunto a todas las rutas. Sin token / token inválido → 401; sin el scope → 403.
- **CORS**: orígenes `https://<url-del-frontend>` y `http://localhost:4200`; métodos `GET, POST, OPTIONS`;
  headers `Authorization, Content-Type`.
