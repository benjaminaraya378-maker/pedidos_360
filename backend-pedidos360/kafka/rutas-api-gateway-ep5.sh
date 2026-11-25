#!/usr/bin/env bash
# Agrega al API Gateway (HTTP API) de EP1-EP4 las rutas de EP5, con el mismo authorizer JWT, y suma PATCH al CORS.
# Es idempotente: si una ruta ya existe, no la vuelve a crear.
#
#   ANY   /api/kafka/{proxy+}          → http://<EC2>:8084/api/kafka/{proxy}         (el rol Admin lo valida el servicio)
#   GET   /api/analitica/{proxy+}      → http://<EC2>:8085/api/analitica/{proxy}     scope Pedidos360.Read
#   PATCH /api/productos/{id}/stock    → http://<EC2>:8081/api/productos/{id}/stock  scope Pedidos360.Write
#
# Uso (con credenciales vigentes del laboratorio en ~/.aws/credentials):
#   API_ID=<id del HTTP API> EC2_HOST=<ip o dns de la EC2> bash kafka/rutas-api-gateway-ep5.sh
# Opcionales: AWS_REGION (us-east-1), AUTHORIZER_ID (por defecto, el primer authorizer JWT del API),
#   SIMULAR=1 muestra los comandos sin llamar a AWS.
set -euo pipefail
export MSYS_NO_PATHCONV=1
: "${API_ID:?falta API_ID (consola de API Gateway → el HTTP API de Pedidos360)}"
: "${EC2_HOST:?falta EC2_HOST (IP o DNS público de la EC2 pedidos360-backend)}"
REGION="${AWS_REGION:-us-east-1}"

if [ "${SIMULAR:-0}" = 1 ]; then
  awscli() {
    echo "  [simulado] aws $*" >&2
    case "$*" in
      *get-authorizers*) echo "auth123" ;;
      *get-routes*) echo "GET /api/bff/me	ANY /api/rabbit/{proxy+}" ;;
      *create-integration*) echo "integ$RANDOM" ;;
      *create-route*) echo "ruta$RANDOM" ;;
      *"get-api "*) echo '{"AllowOrigins":["http://localhost:4200"],"AllowMethods":["GET","POST","DELETE","OPTIONS"],"AllowHeaders":["authorization","content-type"]}' ;;
      *get-stages*) echo "True" ;;
    esac
  }
else
  awscli() { aws --region "$REGION" "$@"; }
fi

AUTH_ID="${AUTHORIZER_ID:-$(awscli apigatewayv2 get-authorizers --api-id "$API_ID" \
  --query "Items[?AuthorizerType=='JWT'] | [0].AuthorizerId" --output text)}"
if [ -z "$AUTH_ID" ] || [ "$AUTH_ID" = None ]; then
  echo "El API $API_ID no tiene un authorizer JWT; se crea en EP1 (ver backend-pedidos360/README.md)"; exit 1
fi
echo "Authorizer JWT: $AUTH_ID"
EXISTENTES="$(awscli apigatewayv2 get-routes --api-id "$API_ID" --query 'Items[].RouteKey' --output text | tr '\t' '\n')"

crear_ruta() { # clave uri método-integración scopes
  local clave="$1" uri="$2" metodo="$3" scopes="$4"
  if grep -qxF "$clave" <<<"$EXISTENTES"; then
    echo "  ya existe: $clave"
    return
  fi
  local integracion
  integracion="$(awscli apigatewayv2 create-integration --api-id "$API_ID" --integration-type HTTP_PROXY \
    --integration-method "$metodo" --integration-uri "$uri" --payload-format-version 1.0 \
    --query IntegrationId --output text)"
  local args=(--api-id "$API_ID" --route-key "$clave" --target "integrations/$integracion"
    --authorization-type JWT --authorizer-id "$AUTH_ID")
  # shellcheck disable=SC2206
  [ -n "$scopes" ] && args+=(--authorization-scopes $scopes)
  awscli apigatewayv2 create-route "${args[@]}" --query RouteId --output text >/dev/null
  echo "  creada: $clave → $uri"
}

echo "Rutas:"
crear_ruta "ANY /api/kafka/{proxy+}" "http://$EC2_HOST:8084/api/kafka/{proxy}" ANY ""
crear_ruta "GET /api/analitica/{proxy+}" "http://$EC2_HOST:8085/api/analitica/{proxy}" GET "Pedidos360.Read"
crear_ruta "PATCH /api/productos/{id}/stock" "http://$EC2_HOST:8081/api/productos/{id}/stock" PATCH "Pedidos360.Write"

echo "CORS:"
CORS="$(awscli apigatewayv2 get-api --api-id "$API_ID" --query CorsConfiguration --output json)"
NUEVO="$(python -c '
import json, sys
c = json.loads(sys.argv[1] or "{}") or {}
metodos = [m.upper() for m in c.get("AllowMethods", [])]
for m in ("GET", "POST", "PATCH", "DELETE", "OPTIONS"):
    if m not in metodos and "*" not in metodos:
        metodos.append(m)
c["AllowMethods"] = metodos
print(json.dumps(c))
' "$CORS")"
if [ "$NUEVO" != "$(python -c 'import json,sys; print(json.dumps(json.loads(sys.argv[1] or "{}") or {}))' "$CORS")" ]; then
  awscli apigatewayv2 update-api --api-id "$API_ID" --cors-configuration "$NUEVO" >/dev/null
  echo "  métodos permitidos: $(python -c 'import json,sys; print(", ".join(json.loads(sys.argv[1])["AllowMethods"]))' "$NUEVO")"
else
  echo "  sin cambios"
fi

AUTO="$(awscli apigatewayv2 get-stages --api-id "$API_ID" --query "Items[?StageName=='\$default'] | [0].AutoDeploy" --output text)"
if [ "$AUTO" != True ]; then
  awscli apigatewayv2 create-deployment --api-id "$API_ID" --stage-name '$default' >/dev/null
  echo "Stage \$default desplegado"
fi
echo "Listo. Probar: curl -i https://$API_ID.execute-api.$REGION.amazonaws.com/api/kafka/topics (401 sin token)"
