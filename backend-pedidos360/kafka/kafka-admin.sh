#!/usr/bin/env bash
# Operaciones de administración de Kafka con las herramientas oficiales (kafka-topics, kafka-configs,
# kafka-consumer-groups), ejecutadas dentro del broker kafka1. Son replicables: el mismo comando da el mismo resultado
# en local y en la EC2. Para hacer lo mismo por HTTP con JWT, ver ms-kafka-admin.
#
# Uso (desde backend-pedidos360, con docker-compose.kafka.yml arriba):
#   bash kafka/kafka-admin.sh cluster                          quórum de Zookeeper, brokers registrados y controlador
#   bash kafka/kafka-admin.sh crear-topicos                    crea los 4 tópicos del sistema si no existen
#   bash kafka/kafka-admin.sh describir [tópico]               particiones, líder, réplicas, ISR y configuración
#   bash kafka/kafka-admin.sh grupos                           consumer groups con offsets, lag y consumidor por partición
#   bash kafka/kafka-admin.sh grupo <grupo>                    un grupo: miembros y lag por partición
#   bash kafka/kafka-admin.sh retencion <tópico> <ms>          cambia retention.ms en caliente
#   bash kafka/kafka-admin.sh particiones <tópico> <total>     aumenta las particiones
#   bash kafka/kafka-admin.sh reset <grupo> <tópico> <inicio|fin|fecha AAAA-MM-DDTHH:MM:SS.000>
#                                                              mueve los offsets (el grupo debe estar detenido)
#   bash kafka/kafka-admin.sh mensajes <tópico> [n]            muestra los primeros n mensajes con clave y headers
#   bash kafka/kafka-admin.sh tolerancia                       detiene kafka3, muestra el ISR reducido y que se sigue
#                                                              escribiendo; luego lo vuelve a levantar
set -uo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash en Windows: no convertir rutas como /brokers/ids

BROKER="${KAFKA_CONTAINER:-kafka1}"
BOOTSTRAP="kafka1:29092,kafka2:29092,kafka3:29092"
# Mismos parámetros que application.yml de ms-pedidos (pedidos) y ms-productos (inventario)
TOPICO_PEDIDOS="pedidos360.pedidos"
TOPICO_INVENTARIO="pedidos360.inventario"

dk() { if docker info >/dev/null 2>&1; then docker "$@"; else sudo docker "$@"; fi; }
# grep -a: los headers de un DLT traen bytes (partición y offset de origen) y no deben cortar la salida
k() { dk exec "$BROKER" "$@" 2>&1 | grep -av "^SLF4J\|WARN \[AdminClient\|could not be established\|^Processed a total"; }
titulo() { echo; echo "== $* =="; }

cmd_cluster() {
  titulo "Ensamble Zookeeper (quórum con 2 de 3)"
  for zk in zookeeper1 zookeeper2 zookeeper3; do
    modo="$(dk exec "$zk" bash -c 'exec 3<>/dev/tcp/127.0.0.1/2181; echo srvr >&3; cat <&3' 2>/dev/null | grep Mode || echo 'Mode: caído')"
    echo "  $zk  $modo"
  done
  titulo "Brokers registrados en Zookeeper"
  k zookeeper-shell zookeeper1:2181 ls /brokers/ids | tail -1 | sed 's/^/  ids: /'
  titulo "Controlador del clúster"
  k zookeeper-shell zookeeper1:2181 get /controller | grep brokerid | sed 's/^/  /'
}

crear() { # nombre particiones replicas config...
  local nombre="$1" particiones="$2" replicas="$3"; shift 3
  local args=()
  for c in "$@"; do args+=(--config "$c"); done
  k kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$nombre" \
    --partitions "$particiones" --replication-factor "$replicas" "${args[@]}"
}

cmd_crear_topicos() {
  titulo "Tópicos del sistema (3 particiones, 3 réplicas)"
  crear "$TOPICO_PEDIDOS" 3 3 cleanup.policy=delete retention.ms=604800000 min.insync.replicas=2
  crear "$TOPICO_INVENTARIO" 3 3 cleanup.policy=compact min.compaction.lag.ms=604800000 segment.ms=86400000 \
    min.insync.replicas=2
  crear "$TOPICO_PEDIDOS.dlt" 3 3 cleanup.policy=delete retention.ms=1209600000 min.insync.replicas=2
  crear "$TOPICO_INVENTARIO.dlt" 3 3 cleanup.policy=delete retention.ms=1209600000 min.insync.replicas=2
  k kafka-topics --bootstrap-server "$BOOTSTRAP" --list | grep '^pedidos360\.' | sed 's/^/  /'
}

cmd_describir() {
  local topicos=("${1:-}")
  [ -z "${1:-}" ] && topicos=("$TOPICO_PEDIDOS" "$TOPICO_INVENTARIO" "$TOPICO_PEDIDOS.dlt" "$TOPICO_INVENTARIO.dlt")
  for t in "${topicos[@]}"; do
    titulo "$t"
    k kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$t" | sed 's/^/  /'
    echo "  -- configuración fijada en el tópico:"
    k kafka-configs --bootstrap-server "$BOOTSTRAP" --entity-type topics --entity-name "$t" --describe \
      | tr ' ' '\n' | grep -E '^(cleanup.policy|retention.ms|min.insync.replicas|min.compaction.lag.ms|segment.ms)=' \
      | sed 's/^/     /'
  done
}

cmd_grupos() {
  titulo "Consumer groups (offset confirmado, offset final, lag y consumidor de cada partición)"
  k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --all-groups
}

cmd_grupo() {
  [ $# -ge 1 ] || { echo "uso: grupo <grupo>"; exit 2; }
  titulo "Grupo $1: miembros"
  k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$1" --members --verbose
  titulo "Grupo $1: offsets y lag"
  k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$1"
}

cmd_retencion() {
  [ $# -ge 2 ] || { echo "uso: retencion <tópico> <ms>"; exit 2; }
  k kafka-configs --bootstrap-server "$BOOTSTRAP" --entity-type topics --entity-name "$1" --alter \
    --add-config "retention.ms=$2"
  cmd_describir "$1"
}

cmd_particiones() {
  [ $# -ge 2 ] || { echo "uso: particiones <tópico> <total>"; exit 2; }
  k kafka-topics --bootstrap-server "$BOOTSTRAP" --alter --topic "$1" --partitions "$2"
  cmd_describir "$1"
}

cmd_reset() {
  [ $# -ge 3 ] || { echo "uso: reset <grupo> <tópico> <inicio|fin|fecha AAAA-MM-DDTHH:MM:SS.000>"; exit 2; }
  local destino
  case "$3" in
    inicio) destino=(--to-earliest) ;;
    fin) destino=(--to-latest) ;;
    fecha) destino=(--to-datetime "${4:?falta la fecha}") ;;
    *) echo "estrategia inválida: $3"; exit 2 ;;
  esac
  k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --group "$1" --topic "$2" --reset-offsets "${destino[@]}" \
    --execute
}

cmd_mensajes() {
  [ $# -ge 1 ] || { echo "uso: mensajes <tópico> [n]"; exit 2; }
  k kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --topic "$1" --from-beginning --max-messages "${2:-5}" \
    --timeout-ms 10000 --property print.key=true --property print.partition=true --property print.offset=true \
    --property print.headers=true
}

cmd_tolerancia() {
  local prueba="pedidos360.prueba-tolerancia"
  crear "$prueba" 3 3 min.insync.replicas=2 >/dev/null
  titulo "Antes: 3 brokers, ISR completo"
  k kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$prueba" | sed 's/^/  /'
  titulo "Se detiene kafka3"
  dk stop kafka3 >/dev/null && echo "  kafka3 detenido"
  sleep 15
  titulo "Con kafka3 caído: líderes reasignados e ISR con 2 réplicas"
  k kafka-topics --bootstrap-server kafka1:29092,kafka2:29092 --describe --topic "$prueba" | sed 's/^/  /'
  titulo "Escritura con acks=all (min.insync.replicas=2): sigue funcionando"
  echo "evento-durante-la-caida-$(date +%s)" | dk exec -i "$BROKER" kafka-console-producer \
    --bootstrap-server kafka1:29092,kafka2:29092 --topic "$prueba" --producer-property acks=all 2>&1 \
    | grep -v '^>' ; echo "  mensaje escrito"
  titulo "Se vuelve a levantar kafka3"
  dk start kafka3 >/dev/null && echo "  kafka3 iniciado; esperando que se ponga al día..."
  sleep 25
  k kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$prueba" | sed 's/^/  /'
  k kafka-topics --bootstrap-server "$BOOTSTRAP" --delete --topic "$prueba" >/dev/null
}

case "${1:-}" in
  cluster) cmd_cluster ;;
  crear-topicos) cmd_crear_topicos ;;
  describir) shift; cmd_describir "$@" ;;
  grupos) cmd_grupos ;;
  grupo) shift; cmd_grupo "$@" ;;
  retencion) shift; cmd_retencion "$@" ;;
  particiones) shift; cmd_particiones "$@" ;;
  reset) shift; cmd_reset "$@" ;;
  mensajes) shift; cmd_mensajes "$@" ;;
  tolerancia) cmd_tolerancia ;;
  *) sed -n '2,18p' "$0"; exit 2 ;;
esac
