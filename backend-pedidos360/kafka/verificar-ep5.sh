#!/usr/bin/env bash
# Verifica la arquitectura Kafka de EP5 de punta a punta y deja los resultados listos para capturar.
#
#   1. Zookeeper: 3 nodos en quórum (1 líder, 2 seguidores).
#   2. Brokers: 3 registrados (ids 1, 2 y 3).
#   3. Tópicos: pedidos360.pedidos y pedidos360.inventario con 3 particiones, 3 réplicas, ISR completo y su
#      configuración (retención, cleanup policy, min.insync.replicas); sus DLT existen.
#   4. Consumer groups: inventario, estado-pedidos y analítica, con su lag.
#   5. Kafka UI responde en el puerto 8090.
#   6. Flujo (solo si GATEWAY y TOKEN_CLIENTE están definidos): un pedido nuevo pasa solo de CREADO a EN_PREPARACION.
#
# Uso (desde backend-pedidos360, con el clúster y los microservicios arriba):
#   bash kafka/verificar-ep5.sh
#   GATEWAY=https://<id>.execute-api.<region>.amazonaws.com TOKEN_CLIENTE=<token> bash kafka/verificar-ep5.sh
# Variables opcionales: PRODUCTO_ID (por defecto 1), KAFKA_UI_URL (por defecto http://localhost:8090).
set -uo pipefail
export MSYS_NO_PATHCONV=1

BOOTSTRAP="kafka1:29092,kafka2:29092,kafka3:29092"
TOPICOS=(pedidos360.pedidos pedidos360.inventario)
GRUPOS=(pedidos360-inventario pedidos360-estado-pedidos pedidos360-analitica)

dk() { if docker info >/dev/null 2>&1; then docker "$@"; else sudo docker "$@"; fi; }
k() { dk exec kafka1 "$@" 2>/dev/null | tr -d '\r'; }

fallos=0
ok()      { echo "  [OK]       $*"; }
falla()   { echo "  [FALLA]    $*"; fallos=$((fallos + 1)); }
omitido() { echo "  [OMITIDO]  $*"; }
titulo()  { echo; echo "== $* =="; }

titulo "1. Ensamble Zookeeper"
lideres=0; seguidores=0
for zk in zookeeper1 zookeeper2 zookeeper3; do
  modo="$(dk exec "$zk" bash -c 'exec 3<>/dev/tcp/127.0.0.1/2181; echo srvr >&3; cat <&3' 2>/dev/null | sed -n 's/^Mode: //p' | tr -d '\r')"
  case "$modo" in
    leader) lideres=$((lideres + 1)); ok "$zk es líder" ;;
    follower) seguidores=$((seguidores + 1)); ok "$zk es seguidor" ;;
    *) falla "$zk no responde o no está en el quórum" ;;
  esac
done
[ "$lideres" = 1 ] && [ "$seguidores" = 2 ] && ok "quórum: 1 líder y 2 seguidores" || falla "el ensamble no tiene 1 líder y 2 seguidores"

titulo "2. Brokers"
ids="$(k zookeeper-shell zookeeper1:2181 ls /brokers/ids | tail -1)"
echo "  brokers registrados: $ids"
for id in 1 2 3; do
  grep -qE "(\[|, )$id(,|\])" <<<"$ids" && ok "broker $id registrado" || falla "broker $id no está registrado"
done

titulo "3. Tópicos"
for t in "${TOPICOS[@]}"; do
  desc="$(k kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$t")"
  if [ -z "$desc" ]; then falla "no existe $t"; continue; fi
  echo "$desc" | sed 's/^/    /'
  grep -q "PartitionCount: 3" <<<"$desc" && ok "$t tiene 3 particiones" || falla "$t no tiene 3 particiones"
  grep -q "ReplicationFactor: 3" <<<"$desc" && ok "$t tiene factor de replicación 3" || falla "$t no tiene 3 réplicas"
  isr_incompletos="$(grep -P 'Partition: \d' <<<"$desc" | grep -vcP 'Isr: \d+,\d+,\d+' || true)"
  [ "$isr_incompletos" = 0 ] && ok "$t: las 3 réplicas de cada partición están en sincronía (ISR)" \
    || falla "$t: $isr_incompletos partición(es) con ISR incompleto"
  lideres_distintos="$(grep -oP 'Leader: \d+' <<<"$desc" | sort -u | wc -l)"
  echo "  líderes repartidos en $lideres_distintos broker(s)"
  config="$(k kafka-configs --bootstrap-server "$BOOTSTRAP" --entity-type topics --entity-name "$t" --describe)"
  grep -q "min.insync.replicas=2" <<<"$config" && ok "$t: min.insync.replicas=2" || falla "$t: falta min.insync.replicas=2"
  if [ "$t" = pedidos360.pedidos ]; then
    grep -q "cleanup.policy=delete" <<<"$config" && grep -q "retention.ms=604800000" <<<"$config" \
      && ok "$t: cleanup.policy=delete, retención 7 días" || falla "$t: retención o cleanup policy distintas"
  else
    grep -q "cleanup.policy=compact" <<<"$config" && grep -q "min.compaction.lag.ms=604800000" <<<"$config" \
      && ok "$t: cleanup.policy=compact, sin compactar antes de 7 días" || falla "$t: compactación distinta"
  fi
  dk exec kafka1 kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$t.dlt" >/dev/null 2>&1 \
    && ok "existe $t.dlt" || falla "falta $t.dlt"
done

titulo "4. Consumer groups"
for g in "${GRUPOS[@]}"; do
  estado="$(k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$g" --state | awk 'NR>1 && NF {print $(NF-1)}' | tail -1)"
  miembros="$(k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$g" --state | awk 'NR>1 && NF {print $NF}' | tail -1)"
  lag="$(k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$g" | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')"
  if [ -n "$estado" ]; then ok "$g: estado $estado, $miembros miembro(s), lag total $lag"
  else falla "$g no existe (¿está arriba el microservicio?)"; fi
done

titulo "5. Kafka UI"
codigo="$(curl -s -o /dev/null -w '%{http_code}' "${KAFKA_UI_URL:-http://localhost:8090}/" || true)"
case "$codigo" in
  200|302) ok "Kafka UI responde ($codigo)" ;;
  *) falla "Kafka UI no responde en ${KAFKA_UI_URL:-http://localhost:8090} (código $codigo)" ;;
esac

titulo "6. Flujo de un pedido (API Gateway)"
if [ -z "${GATEWAY:-}" ] || [ -z "${TOKEN_CLIENTE:-}" ]; then
  omitido "define GATEWAY y TOKEN_CLIENTE para crear un pedido y ver su cambio de estado"
else
  GATEWAY="${GATEWAY%/}"
  respuesta="$(curl -s -w '\n%{http_code}' -X POST "$GATEWAY/api/pedidos" \
    -H "Authorization: Bearer $TOKEN_CLIENTE" -H 'Content-Type: application/json' \
    -d "{\"items\":[{\"productoId\":${PRODUCTO_ID:-1},\"cantidad\":1}]}")"
  codigo="$(tail -1 <<<"$respuesta")"
  id="$(head -1 <<<"$respuesta" | grep -oP '"id":\s*\K\d+' | head -1)"
  if [ "$codigo" != 201 ] || [ -z "$id" ]; then
    falla "crear pedido → $codigo (se esperaba 201)"
  else
    ok "pedido $id creado (estado CREADO)"
    estado=""
    for _ in $(seq 1 20); do
      estado="$(curl -s -H "Authorization: Bearer $TOKEN_CLIENTE" "$GATEWAY/api/pedidos/$id" | grep -oP '"estado":\s*"\K[A-Z_]+')"
      [ "$estado" != CREADO ] && break
      sleep 0.5
    done
    case "$estado" in
      EN_PREPARACION) ok "pedido $id pasó solo a EN_PREPARACION (stock descontado por Kafka)" ;;
      CANCELADO) ok "pedido $id pasó solo a CANCELADO (stock insuficiente)" ;;
      *) falla "pedido $id sigue en $estado después de 10 s" ;;
    esac
  fi
fi

echo
if [ "$fallos" -eq 0 ]; then echo "Resultado: todo OK"; else echo "Resultado: $fallos falla(s)"; fi
exit "$fallos"
