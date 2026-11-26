# Pedidos360 — DSY1107 (EP1 encargo + EP2 presentación + EP3 mensajería RabbitMQ + EP4 clúster y auditoría + EP5 streaming con Kafka + EP6 presentación)

Estado: EP5 (integración de Kafka) y EP6 (presentación) entregados en local, sin AWS: el laboratorio de EP1-EP4 ya no
está activo. Keycloak y Kong en Docker reemplazan el IDaaS y el API Gateway de la nube (ver `demo-local/LEEME.md`).

Un repositorio GitHub con una carpeta por tecnología, cada una con su propio `.gitignore`:

- `frontend-pedidos360/` → Angular + MSAL (login con Entra, guards, MsalInterceptor, consumo vía API Gateway). `npm run demo-local` lo levanta sin el tenant de Entra, con el IDaaS local de prueba.
- `backend-pedidos360/` → microservicios Spring Boot (`bff-pedidos360`, `ms-productos`, `ms-pedidos`, `ms-rabbitmq-admin`) con filtros JWT, para EC2 + RDS; `ms-pedidos` publica y consume eventos en RabbitMQ (3 colas con su DLQ: notificaciones, comprobantes y auditoría) y `ms-rabbitmq-admin` administra colas, exchanges y bindings. RabbitMQ corre como clúster de 2 nodos con `docker-compose.yml` (ver `backend-pedidos360/CAMBIOS_EP4.md`).
  Desde EP5, Kafka (3 Zookeeper + 3 brokers, `docker-compose.kafka.yml`, y Kafka UI, `docker-compose.kafka-ui.yml`) lleva los eventos de pedidos e inventario: `ms-productos` descuenta el stock, `ms-pedidos` confirma o cancela el pedido, `ms-analitica` calcula métricas en tiempo real y `ms-kafka-admin` administra tópicos y consumer groups (ver `backend-pedidos360/CAMBIOS_EP5.md`).

- `demo-local/` → todo el sistema en un solo PC, sin AWS ni Entra: IDaaS local de prueba (firma los JWT y publica su JWKS), arranque de los microservicios y guion de la demo (ver `demo-local/LEEME.md`). Con `backend-pedidos360/docker-compose.nube-local.yml`, Keycloak (IDaaS) y Kong (API Manager) en Docker hacen de Entra External ID y de AWS API Gateway, y `npm run local-completo` usa el frontend con los dos.

Cada carpeta tiene su README con la configuración necesaria y la tabla de rutas/CORS del API Gateway.
