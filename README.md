# Pedidos360 — DSY1107 (EP1 encargo + EP2 presentación + EP3 mensajería RabbitMQ)

Dos repositorios GitHub, uno por tecnología:

- `frontend-pedidos360/` → Angular + MSAL (login con Entra, guards, MsalInterceptor, consumo vía API Gateway).
- `backend-pedidos360/` → microservicios Spring Boot (`bff-pedidos360`, `ms-productos`, `ms-pedidos`, `ms-rabbitmq-admin`) con filtros JWT, para EC2 + RDS; `ms-pedidos` publica y consume eventos en RabbitMQ (con DLQ) y `ms-rabbitmq-admin` administra colas, exchanges y bindings.

Cada carpeta tiene su README con la configuración necesaria y la tabla de rutas/CORS del API Gateway.
