#!/usr/bin/env bash
# Verifica la mensajería de EP4 de punta a punta y deja los resultados listos para capturar.
#
#   1. Clúster: los dos nodos (rabbit@rabbitmq1 y rabbit@rabbitmq2) están en ejecución.
#   2. Topología: 2 exchanges (topic y direct), 6 colas (3 con su DLQ) y sus bindings, creados por ms-pedidos.
#   3. Flujo (solo si GATEWAY y TOKEN_CLIENTE están definidos): JWT inválido → 401, JWT válido → 200, y un
#      pedido nuevo deja en el log las tres líneas: notificación, comprobante y AUDITORIA.
#
# Uso (desde backend-pedidos360, con el clúster y los microservicios arriba):
#   export GATEWAY=https://<id>.execute-api.<region>.amazonaws.com
#   export TOKEN_CLIENTE=<access token de un usuario Cliente>
#   bash rabbitmq/verificar-ep4.sh
#
# Variables opcionales: PRODUCTO_ID (producto a pedir, por defecto 1), LOG_PEDIDOS (log de ms-pedidos, por defecto
# nohup.out), RABBITMQ_CONTAINER (nodo donde se consulta, por defecto rabbitmq1).
set -uo pipefail

NODO="${RABBITMQ_CONTAINER:-rabbitmq1}"
PRODUCTO_ID="${PRODUCTO_ID:-1}"
LOG_PEDIDOS="${LOG_PEDIDOS:-nohup.out}"
EXCHANGE="pedidos360.pedidos.exchange"
DLX="pedidos360.pedidos.dlx"
CASOS=(notificaciones comprobantes auditoria)

# En la EC2 docker suele requerir sudo
dk() { if docker info >/dev/null 2>&1; then docker "$@"; else sudo docker "$@"; fi; }
ctl() { dk exec "$NODO" rabbitmqctl -q "$@" 2>/dev/null | tr -d '\r'; }

fallos=0
ok()      { echo "  [OK]       $*"; }
falla()   { echo "  [FALLA]    $*"; fallos=$((fallos + 1)); }
omitido() { echo "  [OMITIDO]  $*"; }
titulo()  { echo; echo "== $* =="; }

titulo "1. Clúster RabbitMQ"
estado="$(ctl cluster_status --formatter json | tr -d ' \n')"
corriendo="$(grep -o '"running_nodes":\[[^]]*\]' <<<"$estado" || true)"
echo "  $corriendo"
for nodo in rabbit@rabbitmq1 rabbit@rabbitmq2; do
  if grep -q "$nodo" <<<"$corriendo"; then ok "$nodo en ejecución"; else falla "$nodo no está en el clúster"; fi
done

titulo "2. Exchanges"
exchanges="$(ctl list_exchanges name type)"
grep -E "^pedidos360\." <<<"$exchanges" | sed 's/^/  /'
grep -qP "^$EXCHANGE\ttopic$" <<<"$exchanges" && ok "$EXCHANGE es topic" || falla "$EXCHANGE no existe o no es topic"
grep -qP "^$DLX\tdirect$" <<<"$exchanges" && ok "$DLX es direct" || falla "$DLX no existe o no es direct"

titulo "3. Colas y DLQ"
colas="$(ctl list_queues name arguments)"
for caso in "${CASOS[@]}"; do
  principal="pedidos360.$caso.queue"
  dlq="pedidos360.$caso.dlq"
  linea="$(grep -P "^$principal\t" <<<"$colas" || true)"
  if [ -z "$linea" ]; then falla "falta la cola $principal"
  elif grep -q "x-dead-letter-exchange" <<<"$linea" && grep -q "$dlq" <<<"$linea"; then ok "$principal → DLX $DLX → $dlq"
  else falla "$principal existe pero no apunta a su DLQ"; fi
  grep -qP "^$dlq\t" <<<"$colas" && ok "existe $dlq" || falla "falta la cola $dlq"
done

titulo "4. Bindings"
bindings="$(ctl list_bindings source_name destination_name routing_key)"
for caso in "${CASOS[@]}"; do
  grep -qP "^$EXCHANGE\tpedidos360\.$caso\.queue\tpedido\.creado$" <<<"$bindings" \
    && ok "$EXCHANGE --pedido.creado--> pedidos360.$caso.queue" || falla "falta el binding de $caso.queue"
  grep -qP "^$DLX\tpedidos360\.$caso\.dlq\tpedidos360\.$caso\.dlq$" <<<"$bindings" \
    && ok "$DLX --pedidos360.$caso.dlq--> pedidos360.$caso.dlq" || falla "falta el binding de $caso.dlq"
done

titulo "5. JWT y flujo de un pedido (API Gateway)"
if [ -z "${GATEWAY:-}" ] || [ -z "${TOKEN_CLIENTE:-}" ]; then
  omitido "define GATEWAY y TOKEN_CLIENTE para probar el JWT y crear un pedido"
else
  GATEWAY="${GATEWAY%/}"
  codigo="$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer token-invalido' "$GATEWAY/api/pedidos")"
  [ "$codigo" = "401" ] && ok "token inválido → 401" || falla "token inválido → $codigo (se esperaba 401)"
  codigo="$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN_CLIENTE" "$GATEWAY/api/pedidos")"
  [ "$codigo" = "200" ] && ok "token válido → 200" || falla "token válido → $codigo (se esperaba 200)"

  respuesta="$(curl -s -w '\n%{http_code}' -X POST "$GATEWAY/api/pedidos" \
    -H "Authorization: Bearer $TOKEN_CLIENTE" -H 'Content-Type: application/json' \
    -d "{\"items\":[{\"productoId\":$PRODUCTO_ID,\"cantidad\":1}],\"observacion\":\"verificar-ep4\"}")"
  codigo="${respuesta##*$'\n'}"
  cuerpo="${respuesta%$'\n'*}"
  pedido="$(grep -o '"id":[0-9]*' <<<"$cuerpo" | head -1 | cut -d: -f2)"
  if [ "$codigo" != "201" ] || [ -z "$pedido" ]; then
    falla "crear pedido → $codigo (se esperaba 201): $cuerpo"
  else
    ok "pedido #$pedido creado (201)"
    if [ ! -r "$LOG_PEDIDOS" ]; then
      omitido "no se puede leer el log '$LOG_PEDIDOS'; define LOG_PEDIDOS para comprobar los tres consumidores"
    else
      for _ in $(seq 1 20); do
        grep -qF "recibimos tu pedido #$pedido " "$LOG_PEDIDOS" \
          && grep -qF "Comprobante del pedido $pedido generado" "$LOG_PEDIDOS" \
          && grep -qF "AUDITORIA | pedido #$pedido " "$LOG_PEDIDOS" && break
        sleep 1
      done
      grep -F -e "recibimos tu pedido #$pedido " -e "Comprobante del pedido $pedido generado" \
        -e "AUDITORIA | pedido #$pedido " "$LOG_PEDIDOS" | tail -3 | sed 's/^/  | /'
      grep -qF "recibimos tu pedido #$pedido " "$LOG_PEDIDOS" && ok "log: notificación" || falla "log: falta la notificación"
      grep -qF "Comprobante del pedido $pedido generado" "$LOG_PEDIDOS" && ok "log: comprobante" || falla "log: falta el comprobante"
      grep -qF "AUDITORIA | pedido #$pedido " "$LOG_PEDIDOS" && ok "log: AUDITORIA" || falla "log: falta la línea AUDITORIA"
    fi
  fi
fi

echo
if [ "$fallos" -eq 0 ]; then echo "Verificación EP4: todo en orden."; else echo "Verificación EP4: $fallos comprobación(es) fallaron."; fi
exit $(( fallos > 0 ))
