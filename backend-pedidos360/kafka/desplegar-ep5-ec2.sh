#!/usr/bin/env bash
# Despliega EP5 en la EC2 pedidos360-backend, sobre la infraestructura de EP1-EP4 (RDS, RabbitMQ, API Gateway).
#   1. Levanta el clúster Kafka (3 Zookeeper + 3 brokers) y Kafka UI con docker compose.
#   2. Compila y (re)inicia ms-productos, ms-pedidos, ms-kafka-admin y ms-analitica con las variables de EP1-EP4.
#   3. Corre kafka/verificar-ep5.sh.
#
# Uso, en la EC2, dentro de backend-pedidos360 y después de git pull:
#   bash kafka/desplegar-ep5-ec2.sh
#
# Variables (en el entorno o en backend-pedidos360/.env, que no se sube a git):
#   JWT_ISSUER, JWT_JWKS_URI, JWT_AUDIENCE, CORS_ALLOWED_ORIGINS   las mismas de EP1-EP4
#   DB_USER, DB_PASSWORD y RDS_ENDPOINT (o DB_URL_PRODUCTOS y DB_URL_PEDIDOS completas)
#   Opcionales: RABBITMQ_ADDRESSES (localhost:5672,localhost:5673), KAFKA_BOOTSTRAP_SERVERS (localhost:9092-9094),
#   KAFKA_UI_USER / KAFKA_UI_PASSWORD (cambiar los de por defecto), HEAP (memoria de cada servicio, 256m)
#
# Memoria: Kafka (3 + 3), Kafka UI, RabbitMQ y 6 servicios Java necesitan unos 5 GB. Usar t3.large o mayor.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; . ./.env; set +a; fi

: "${JWT_ISSUER:?falta JWT_ISSUER (ver EP1-EP4)}"
: "${JWT_JWKS_URI:?falta JWT_JWKS_URI}"
: "${JWT_AUDIENCE:?falta JWT_AUDIENCE}"
: "${DB_USER:?falta DB_USER}"
: "${DB_PASSWORD:?falta DB_PASSWORD}"
if [ -z "${DB_URL_PRODUCTOS:-}" ] || [ -z "${DB_URL_PEDIDOS:-}" ]; then
  : "${RDS_ENDPOINT:?falta RDS_ENDPOINT (o DB_URL_PRODUCTOS y DB_URL_PEDIDOS)}"
  DB_URL_PRODUCTOS="jdbc:mysql://$RDS_ENDPOINT:3306/pedidos360_productos?createDatabaseIfNotExist=true&serverTimezone=UTC"
  DB_URL_PEDIDOS="jdbc:mysql://$RDS_ENDPOINT:3306/pedidos360_pedidos?createDatabaseIfNotExist=true&serverTimezone=UTC"
fi
export JWT_ISSUER JWT_JWKS_URI JWT_AUDIENCE DB_USER DB_PASSWORD
export CORS_ALLOWED_ORIGINS="${CORS_ALLOWED_ORIGINS:-http://localhost:4200}"
export RABBITMQ_ADDRESSES="${RABBITMQ_ADDRESSES:-localhost:5672,localhost:5673}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092,localhost:9093,localhost:9094}"
export PRODUCTOS_URL="${PRODUCTOS_URL:-http://localhost:8081}"
HEAP="${HEAP:-256m}"

dk() { if docker info >/dev/null 2>&1; then docker "$@"; else sudo docker "$@"; fi; }
paso() { echo; echo "== $* =="; }

paso "Memoria disponible"
total_mb="$(free -m | awk '/^Mem:/ {print $2}')"
echo "  total: ${total_mb} MB"
if [ "${total_mb:-0}" -lt 7000 ]; then
  echo "  AVISO: menos de 7 GB. Kafka y los servicios pueden quedarse sin memoria; se recomienda t3.large o mayor."
fi

paso "Clúster Kafka (3 Zookeeper + 3 brokers)"
dk compose version >/dev/null
dk compose -f docker-compose.kafka.yml up -d
for _ in $(seq 1 60); do
  sanos="$(dk compose -f docker-compose.kafka.yml ps --format '{{.Health}}' | grep -c '^healthy$' || true)"
  [ "$sanos" = 6 ] && break
  sleep 5
done
dk compose -f docker-compose.kafka.yml ps
[ "$sanos" = 6 ] || { echo "El clúster no quedó healthy (6/6) en 5 minutos"; exit 1; }

paso "Kafka UI (puerto 8090 solo en 127.0.0.1; abrir con ssh -L 8090:localhost:8090)"
dk compose -f docker-compose.kafka-ui.yml up -d

paso "Compilación"
for ms in ms-productos ms-pedidos ms-kafka-admin ms-analitica; do
  mvn -q -B -f "$ms/pom.xml" -DskipTests package
  echo "  $ms compilado"
done

paso "Microservicios"
mkdir -p logs
iniciar() { # servicio [DB_URL]
  local ms="$1" db="${2:-}"
  pkill -f "$ms-1.0.0.jar" 2>/dev/null && sleep 3 || true
  if [ -n "$db" ]; then
    DB_URL="$db" nohup java -Xmx"$HEAP" -jar "$ms/target/$ms-1.0.0.jar" > "logs/$ms.log" 2>&1 &
  else
    nohup java -Xmx"$HEAP" -jar "$ms/target/$ms-1.0.0.jar" > "logs/$ms.log" 2>&1 &
  fi
  echo "  $ms iniciado (log en logs/$ms.log)"
}
iniciar ms-productos "$DB_URL_PRODUCTOS"
iniciar ms-pedidos "$DB_URL_PEDIDOS"
iniciar ms-kafka-admin
iniciar ms-analitica

# Cada servicio responde 401 sin token cuando ya está arriba
for par in 8081:/api/productos 8082:/api/pedidos 8084:/api/kafka/cluster 8085:/api/analitica/resumen; do
  puerto="${par%%:*}"; ruta="${par#*:}"
  for _ in $(seq 1 60); do
    codigo="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$puerto$ruta" || true)"
    [ "$codigo" = 401 ] && break
    sleep 3
  done
  echo "  :$puerto$ruta → $codigo (401 = arriba y exigiendo JWT)"
done

paso "Verificación"
bash kafka/verificar-ep5.sh || true

cat <<'FIN'

Siguiente:
  - Security group: abrir 8084 y 8085 solo para el API Gateway (igual que 8080-8083). No abrir 9092-9094, 2181 ni 8090.
  - Rutas del API Gateway: desde un equipo con AWS CLI,
      API_ID=<id> EC2_HOST=<ip o dns de la EC2> bash kafka/rutas-api-gateway-ep5.sh
FIN
