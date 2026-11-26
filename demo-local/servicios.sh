#!/usr/bin/env bash
# Arranca (o reinicia) los microservicios en este PC para la demo local, con poca memoria, contra:
#   MySQL 8.4 en 127.0.0.1:3307 · RabbitMQ (clúster EP4) en 5672/5673 · Kafka en 9092-9094 · IDaaS local en 8099
# Requisitos: haber corrido `node demo-local/idaas-local.mjs` (crea idaas.env) y compilado cada servicio con
#   mvn -DskipTests package
# Uso: bash demo-local/servicios.sh [ms-productos ms-pedidos ms-analitica ms-kafka-admin bff-pedidos360]
#      (sin argumentos: los cinco). Logs en demo-local/<servicio>.log
set -u
S="$(cd "$(dirname "$0")" && pwd)"
B="$(cd "$S/../backend-pedidos360" && pwd)"
[ -f "$S/idaas.env" ] || { echo "Falta $S/idaas.env: corre primero  node demo-local/idaas-local.mjs"; exit 1; }
set -a; . "$S/idaas.env"; set +a
export DB_USER="${DB_USER:-root}" DB_PASSWORD="${DB_PASSWORD:-pedidos360}"
export RABBITMQ_ADDRESSES=localhost:5672,localhost:5673
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094
export COMPROBANTES_DIR="$S/comprobantes"
export PRODUCTOS_URL=http://localhost:8081 PEDIDOS_URL=http://localhost:8082
OPCIONES="-Xss512k -XX:+UseSerialGC -XX:TieredStopAtLevel=1 -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8"
MYSQL="jdbc:mysql://127.0.0.1:3307"

detener_puerto() { # cierra lo que escuche en el puerto (Windows con Git Bash, o Linux/macOS)
  local puerto="$1" pid
  if command -v taskkill >/dev/null 2>&1; then
    pid="$(netstat -ano 2>/dev/null | grep -E "(127.0.0.1|0.0.0.0|\[::\]):$puerto " | grep LISTEN | awk '{print $NF}' | head -1)"
    [ -n "$pid" ] && taskkill //PID "$pid" //F >/dev/null 2>&1 && sleep 2
  elif command -v lsof >/dev/null 2>&1; then
    pid="$(lsof -ti "tcp:$puerto" -sTCP:LISTEN 2>/dev/null)"
    [ -n "$pid" ] && kill $pid && sleep 2
  fi
  return 0
}

iniciar() { # servicio puerto heap [DB_URL]
  local ms="$1" puerto="$2" heap="$3" db="${4:-}"
  local jar="$B/$ms/target/$ms-1.0.0.jar"
  [ -f "$jar" ] || { echo "Falta $jar: compila con  (cd backend-pedidos360/$ms && mvn -DskipTests package)"; return 1; }
  detener_puerto "$puerto"
  ( if [ -n "$db" ]; then export DB_URL="$db"; fi
    nohup java -Xmx"$heap" $OPCIONES -jar "$jar" > "$S/$ms.log" 2>&1 & )
  echo "$ms iniciando en :$puerto (log demo-local/$ms.log)"
}

servicios=("$@")
[ ${#servicios[@]} -eq 0 ] && servicios=(ms-productos ms-pedidos ms-analitica ms-kafka-admin bff-pedidos360)
for ms in "${servicios[@]}"; do
  case "$ms" in
    ms-productos) iniciar ms-productos 8081 200m \
      "$MYSQL/pedidos360_productos?createDatabaseIfNotExist=true&serverTimezone=UTC" ;;
    ms-pedidos) iniciar ms-pedidos 8082 200m \
      "$MYSQL/pedidos360_pedidos?createDatabaseIfNotExist=true&serverTimezone=UTC" ;;
    ms-analitica) iniciar ms-analitica 8085 128m ;;
    ms-kafka-admin) iniciar ms-kafka-admin 8084 128m ;;
    bff-pedidos360) iniciar bff-pedidos360 8080 128m ;;
    *) echo "Servicio desconocido: $ms" ;;
  esac
done
