#!/usr/bin/env bash
# Configura Kong (API Manager) igual que el API Gateway de EP1-EP4:
#   - una ruta /api/** por microservicio (host.docker.internal = este PC)
#   - plugin jwt: exige un JWT del tenant de Keycloak válido (firma RS256 con la clave del realm, emisor y vencimiento);
#     sin token o con uno inválido responde 401 sin llegar al microservicio. El rol y el scope los vuelve a
#     validar cada microservicio (403), como en la nube.
#   - plugin cors: el frontend (http://localhost:4200) puede llamar al gateway.
# Uso (desde backend-pedidos360, con docker-compose.nube-local.yml arriba): bash nube-local/kong/configurar-kong.sh
set -euo pipefail

KEYCLOAK="${KEYCLOAK_URL:-http://localhost:8180}"
REALM="${KEYCLOAK_REALM:-pedidos360}"
KONG_ADMIN="${KONG_ADMIN_URL:-http://localhost:8001}"
ISSUER="$KEYCLOAK/realms/$REALM"
SALIDA="$(cd "$(dirname "$0")" && pwd)/kong.generado.yml"

echo "Esperando a Keycloak ($ISSUER)..."
for _ in $(seq 1 60); do
  if curl -sf "$ISSUER" > /dev/null; then break; fi
  sleep 3
done
CLAVE="$(curl -sf "$ISSUER" | sed -n 's/.*"public_key":"\([^"]*\)".*/\1/p')"
[ -n "$CLAVE" ] || { echo "No se pudo leer la clave pública del realm $REALM"; exit 1; }

echo "Esperando a Kong ($KONG_ADMIN)..."
for _ in $(seq 1 40); do
  if curl -sf "$KONG_ADMIN/status" > /dev/null; then break; fi
  sleep 2
done

# La clave del realm en formato PEM (64 caracteres por línea), con la sangría que pide el YAML
PEM="$(echo "$CLAVE" | fold -w 64 | sed 's/^/          /')"

cat > "$SALIDA" <<EOF
# Generado por configurar-kong.sh: no editar a mano (la clave cambia si se recrea el contenedor de Keycloak)
_format_version: "3.0"

services:
  - name: bff-pedidos360
    url: http://host.docker.internal:8080
    routes: [{ name: bff, paths: [/api/bff], strip_path: false }]
  - name: ms-productos
    url: http://host.docker.internal:8081
    routes: [{ name: productos, paths: [/api/productos], strip_path: false }]
  - name: ms-pedidos
    url: http://host.docker.internal:8082
    routes: [{ name: pedidos, paths: [/api/pedidos], strip_path: false }]
  - name: ms-kafka-admin
    url: http://host.docker.internal:8084
    routes: [{ name: kafka, paths: [/api/kafka], strip_path: false }]
  - name: ms-analitica
    url: http://host.docker.internal:8085
    routes: [{ name: analitica, paths: [/api/analitica], strip_path: false }]

plugins:
  - name: cors
    config:
      origins: [http://localhost:4200]
      methods: [GET, POST, PUT, PATCH, DELETE, OPTIONS]
      headers: [Authorization, Content-Type]
      max_age: 3600
  - name: jwt
    config:
      key_claim_name: iss          # el emisor del token elige la credencial (el tenant de Keycloak)
      claims_to_verify: [exp]      # además de la firma: rechaza tokens vencidos
      run_on_preflight: false      # los OPTIONS de CORS no llevan token

consumers:
  - username: tenant-pedidos360
    jwt_secrets:
      - key: $ISSUER
        algorithm: RS256
        rsa_public_key: |
          -----BEGIN PUBLIC KEY-----
$PEM
          -----END PUBLIC KEY-----
EOF

curl -sf -X POST "$KONG_ADMIN/config" -F "config=@$SALIDA" > /dev/null
echo "Kong configurado: 5 rutas /api/**, JWT del tenant $REALM (RS256) y CORS para http://localhost:4200"
echo "Consola: http://localhost:8002 · Gateway: http://localhost:8000"
