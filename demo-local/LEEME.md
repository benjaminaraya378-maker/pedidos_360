# Pedidos360 en local, sin AWS (demo de EP5 y EP6)

Levanta todo el sistema en un solo PC: RabbitMQ (EP4), Kafka con Zookeeper y Kafka UI (EP5), MySQL, los microservicios y
el frontend. No usa la cuenta de AWS ni el tenant de Entra: un **IDaaS local de prueba** (`idaas-local.mjs`) firma los
JWT con los mismos claims que Entra External ID (`oid`, `roles`, `scp`, `name`, `email`) y publica su JWKS; los
microservicios validan firma, emisor, audiencia, vigencia, rol y scope exactamente igual que en la nube.

La sección 8 reemplaza además el IDaaS y el API Gateway por Keycloak y Kong en Docker (la "nube local").

**Requisitos:** Docker Desktop abierto, Java 21, Maven, Node 20+ y Git Bash (en Windows). Unos 5 GB de RAM libres.
Todos los comandos se corren desde la raíz del repositorio.

## 1. Infraestructura

```bash
cd backend-pedidos360
docker compose up -d                                   # RabbitMQ de EP4 (2 nodos)
docker compose -f docker-compose.kafka.yml up -d       # 3 Zookeeper + 3 brokers (esperar 6 "healthy" con ... ps)
docker compose -f docker-compose.kafka-ui.yml up -d    # Kafka UI en http://localhost:8090 (admin / pedidos360)
docker run -d --name pedidos360-mysql -p 127.0.0.1:3307:3306 -e MYSQL_ROOT_PASSWORD=pedidos360 mysql:8.4
#   (si ya existe:  docker start pedidos360-mysql)
cd ..
```

## 2. IDaaS local (en otra terminal; dejarla abierta)

```bash
node demo-local/idaas-local.mjs
```

Escribe `admin.jwt`, `cliente.jwt` e `idaas.env` en esta carpeta, publica el JWKS en `http://127.0.0.1:8099/keys` y
entrega tokens nuevos en `/idaas/token?usuario=admin|cliente`. La clave RSA se guarda en `clave-local.pem` y los usuarios
tienen un `oid` fijo, así que al reiniciar el IDaaS los tokens siguen valiendo y cada usuario ve sus pedidos. Ninguno de
esos archivos se sube a git (`.gitignore`). Los tokens duran 12 horas.

## 3. Microservicios

```bash
for ms in ms-productos ms-pedidos ms-analitica ms-kafka-admin bff-pedidos360; do
  (cd backend-pedidos360/$ms && mvn -q -DskipTests package)
done
bash demo-local/servicios.sh          # 8081 productos · 8082 pedidos · 8085 analítica · 8084 kafka-admin · 8080 BFF
```

Cada servicio deja su log en `demo-local/<servicio>.log`. `bash demo-local/servicios.sh ms-analitica` reinicia solo uno.

## 4. Frontend

```bash
cd frontend-pedidos360
npm install
npm run demo-local                    # http://localhost:4200
```

«Iniciar sesión» entra como **Admin** (Benjamin Araya). Para entrar como **Cliente** (Oscar Baez), abrir
`http://localhost:4200/?usuario=cliente` en otra ventana privada y pulsar «Iniciar sesión». La barra muestra la etiqueta
«Demo local». El dev-server hace de API Gateway: reenvía `/api/**` a cada microservicio y `/idaas/**` al IDaaS local
(`proxy.demo-local.json`).

## 5. Verificación

```bash
cd backend-pedidos360
bash kafka/verificar-ep5.sh                                                                   # 23 comprobaciones
GATEWAY=http://localhost:8082 TOKEN_CLIENTE=$(cat ../demo-local/cliente.jwt) bash kafka/verificar-ep5.sh   # 25, con un pedido
bash rabbitmq/verificar-ep4.sh                                                               # RabbitMQ de EP4 intacto
```

## 6. Guion de la demo (lámina 15, unos 3 minutos y medio)

Con la nube local (sección 8), que es lo que muestra la lámina 15, el paso 4 se hace por Kong (`:8000`) con el token de
Keycloak (comandos en la sección 8) y en el frontend se inicia sesión en Keycloak; el resto es igual. Si no alcanza la
memoria, se usa el IDaaS local tal como está escrito aquí.

1. `docker ps --format "table {{.Names}}\t{{.Status}}"` → 3 Zookeeper, 3 brokers, Kafka UI y los 2 nodos de RabbitMQ
   *healthy* / *Up*.
2. Consola de RabbitMQ en http://localhost:15672 (guest / guest): *Overview* → *Nodes* (rabbit@rabbitmq1 y
   rabbit@rabbitmq2), *Queues* (3 colas con DLX/DLK y sus 3 DLQ) y *Exchanges* (`pedidos360.pedidos.exchange` topic y
   `pedidos360.pedidos.dlx` direct).
3. Kafka UI → *Brokers* (3 brokers, controlador, URP 0) → *Topics* (los 2 tópicos y sus DLT, 3 particiones y 3
   réplicas) → `pedidos360.inventario` → *Settings* (`cleanup.policy=compact`, `min.compaction.lag.ms`).
4. El JWT, desde la raíz del repositorio (rechaza inválidos, acepta válidos):
   ```bash
   curl -s -o /dev/null -w "sin token: %{http_code}\n" http://localhost:8084/api/kafka/topics
   curl -s -o /dev/null -w "Cliente: %{http_code}\n" -H "Authorization: Bearer $(cat demo-local/cliente.jwt)" http://localhost:8084/api/kafka/topics
   curl -s -o /dev/null -w "Admin: %{http_code}\n" -H "Authorization: Bearer $(cat demo-local/admin.jwt)" http://localhost:8084/api/kafka/topics
   ```
   → 401, 403 y 200.

Luego, en el frontend y en Kafka UI:

5. Frontend como Cliente: *Catálogo* → «Agregar al pedido» → *Pedidos* → «Confirmar pedido». El pedido aparece en
   CREADO y, sin recargar, pasa a EN_PREPARACION.
6. Kafka UI → *Messages* del tópico de pedidos: el evento con su clave, partición, offset y JSON.
7. Frontend como Admin → *Analítica*: el pedido en «Últimos eventos» con tópico, partición y offset.
8. Detener la analítica (cerrar su proceso java o `taskkill` del PID que escucha en 8085), crear 2 o 3 pedidos y abrir
   *Kafka* → «Detalle» del grupo `pedidos360-analitica`: EMPTY y lag por partición. Lo mismo en Kafka UI → *Consumers*
   o con `bash kafka/kafka-admin.sh grupo pedidos360-analitica` (desde backend-pedidos360). Para no esperar en vivo,
   relanzarla con `bash demo-local/servicios.sh ms-analitica` mientras se muestra Kafka UI: el grupo vuelve a STABLE
   y el lag a 0.

Si algo falla en vivo, las láminas 3, 9, 10 y 12 y la sección 12 del informe muestran los mismos pasos con capturas
reales. Si el tiempo aprieta, se puede saltar el paso del evento en Kafka UI (*Messages*): ya aparece en la lámina 10.

## 7. Apagar

Cerrar los procesos java (o `taskkill` por puerto), el IDaaS y `ng serve`; luego
`docker compose -f docker-compose.kafka-ui.yml stop`, `docker compose -f docker-compose.kafka.yml stop`,
`docker compose stop`, `docker compose -f docker-compose.nube-local.yml stop` (si se usó la sección 8),
`docker stop pedidos360-mysql`, y cerrar Docker Desktop. Los volúmenes se conservan.

## 8. Nube local: Keycloak (IDaaS) y Kong (API Manager)

Reemplaza en el PC los dos servicios de nube que en EP1-EP4 no eran Docker: **Keycloak** hace de Entra External ID
(tenant, usuarios, roles y login OAuth 2.0 / OpenID Connect con Authorization Code + PKCE) y **Kong** de AWS API Gateway
(rutas `/api/**`, validación del JWT y CORS). Con esto no se usa el IDaaS local de la sección 2. Suma ~600 MB de RAM.

```bash
cd backend-pedidos360
docker compose -f docker-compose.nube-local.yml up -d   # Keycloak :8180 · Kong :8000 (proxy), :8001 (admin), :8002 (Kong Manager)
bash nube-local/kong/configurar-kong.sh                  # 5 rutas, plugin jwt con la clave RS256 del tenant y cors
cd ..
IDAAS=keycloak bash demo-local/servicios.sh              # los microservicios validan los JWT de Keycloak (keycloak.env)
cd frontend-pedidos360 && npm run local-completo         # http://localhost:4200, etiqueta «Local: Keycloak + Kong»
```

- Usuarios del tenant: `admin` / `Admin.360` (Benjamin Araya, rol Admin) y `cliente` / `Cliente.360` (Oscar Baez, rol
  Cliente). «Iniciar sesión» lleva a la pantalla de Keycloak y vuelve con la sesión; «Cerrar sesión» la cierra en Keycloak.
- Consolas: Keycloak http://localhost:8180/admin (`admin` / `admin`) → realm *pedidos360* → *Users*, *Realm roles*,
  *Clients*. Kong Manager http://localhost:8002 → *Gateway Services*, *Routes*, *Plugins*.
- `configurar-kong.sh` se vuelve a correr si se recrea el contenedor de Kong o el de Keycloak (Kong no guarda la
  configuración y la clave del tenant cambia si Keycloak parte de cero).
- Con `IDAAS=keycloak` los servicios rechazan los tokens del IDaaS local (otro emisor); para volver al modo de las
  secciones 2 a 4, reiniciarlos con `bash demo-local/servicios.sh`.

Pruebas por Kong con curl (desde cualquier carpeta):

```bash
TOKEN=$(curl -s -X POST http://localhost:8180/realms/pedidos360/protocol/openid-connect/token \
  -d grant_type=password -d client_id=pedidos360-cli -d username=cliente -d password=Cliente.360 \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
curl -s -o /dev/null -w "sin token: %{http_code}\n" http://localhost:8000/api/productos                       # 401 (Kong)
curl -s -o /dev/null -w "con token: %{http_code}\n" -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/productos   # 200
curl -s -o /dev/null -w "Cliente en ruta Admin: %{http_code}\n" -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/kafka/topics   # 403
```

Outbox (opcional, ~1 minuto): con los servicios arriba, `docker stop kafka1 kafka2 kafka3`, crear un pedido desde el
frontend (responde al tiro y queda en CREADO), mostrar la fila con
`docker exec pedidos360-mysql mysql -uroot -ppedidos360 -e "SELECT clave, intentos, ultimo_error FROM pedidos360_pedidos.eventos_kafka_pendientes"`,
y `docker start kafka1 kafka2 kafka3`: en 10 a 30 s el relé lo publica y el pedido pasa solo a EN_PREPARACION.

DLT (rol Admin): `GET http://localhost:8000/api/kafka/dlt/pedidos360.pedidos.dlt?max=5` muestra lo pendiente sin
consumirlo y `POST .../reprocesar` con `{"max": 1}` lo devuelve a su tópico de origen.

Capturas y transcripciones: sección 7 del informe (outbox y DLT) y sección 13 (Keycloak y Kong).
