# Pedidos360 — DSY1107 (EP1 encargo + EP2 presentación + EP3 mensajería RabbitMQ + EP4 clúster y auditoría)

Dos repositorios GitHub, uno por tecnología:

- `frontend-pedidos360/` → Angular + MSAL (login con Entra, guards, MsalInterceptor, consumo vía API Gateway).
- `backend-pedidos360/` → microservicios Spring Boot (`bff-pedidos360`, `ms-productos`, `ms-pedidos`, `ms-rabbitmq-admin`) con filtros JWT, para EC2 + RDS; `ms-pedidos` publica y consume eventos en RabbitMQ (3 colas con su DLQ: notificaciones, comprobantes y auditoría) y `ms-rabbitmq-admin` administra colas, exchanges y bindings. RabbitMQ corre como clúster de 2 nodos con `docker-compose.yml` (ver `backend-pedidos360/CAMBIOS_EP4.md`).

Cada carpeta tiene su README con la configuración necesaria y la tabla de rutas/CORS del API Gateway.
