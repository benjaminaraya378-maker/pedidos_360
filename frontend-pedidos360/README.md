# Pedidos360 · Frontend (Angular + MSAL)

Inicia sesión contra el tenant de **Microsoft Entra** con OpenID Connect (Authorization Code + PKCE) usando
`@azure/msal-browser` / `@azure/msal-angular`, protege rutas con `MsalGuard`, adjunta el access token con
`MsalInterceptor` y consume el backend a través de **AWS API Gateway**.

```
src/app/
├─ app.config.ts        # router, HttpClient, locale es-CL y los proveedores de autenticación
├─ app.routes.ts        # /productos, /pedidos, /perfil (MsalGuard) · /admin, /analitica, /kafka (MsalGuard + roleGuard('Admin'))
├─ core/auth.providers.ts            # PublicClientApplication, MSAL_GUARD_CONFIG, MSAL_INTERCEPTOR_CONFIG (protectedResourceMap)
├─ core/auth.providers.demo-local.ts # solo en `npm run demo-local`: sesión del IDaaS local en vez de MSAL (core/demo-local/)
├─ core/auth.service.ts # login/logout, acquireTokenSilent, roles y scopes leídos de los claims del access token
├─ core/api.service.ts  # llamadas al API Gateway (/api/productos, /api/pedidos, /api/bff/**, /api/analitica/**, /api/kafka/**)
└─ features/            # home (dashboard BFF), productos, pedidos, perfil (claims del token), admin, analitica (ms-analitica),
                         # kafka (ms-kafka-admin), no-autorizado
```

## Configuración (`src/environments/environment.ts` y `environment.development.ts`)

| Campo | Valor |
|---|---|
| `msal.clientId` | Application (client) ID de la app **Pedidos360-Web** (plataforma SPA) |
| `msal.authority` | `https://<subdominio>.ciamlogin.com/<tenant-id>/` |
| `msal.knownAuthorities` | `['<subdominio>.ciamlogin.com']` |
| `msal.redirectUri` | URL registrada como redirect URI SPA (`http://localhost:4200` o la URL pública del frontend) |
| `api.baseUrl` | Invoke URL del API Gateway (`https://<api-id>.execute-api.<region>.amazonaws.com`) |
| `api.scopes` | `api://<client-id-api>/Pedidos360.Read`, `api://<client-id-api>/Pedidos360.Write` |

En el tenant: app **Pedidos360-API** con scopes `Pedidos360.Read` / `Pedidos360.Write` (Expose an API) y app roles
`Admin` / `Cliente`; app **Pedidos360-Web** (SPA) con permiso a esos scopes; user flow de registro/inicio de sesión
asociado a Pedidos360-Web; roles asignados a los usuarios en Enterprise applications › Pedidos360-API.

## Ejecutar

```bash
npm install
npm start            # http://localhost:4200 (proxy.conf.json envía /api/** a los microservicios locales: bff 8080,
                     # productos 8081, pedidos 8082, kafka-admin 8084, analitica 8085)
npm run build:prod   # dist/frontend-pedidos360/browser → publicar en S3 + CloudFront (o similar con HTTPS)
```

### Modo demo local (sin el tenant de Entra)

```bash
npm run demo-local   # = ng serve --configuration demo-local · http://localhost:4200  (?usuario=cliente para entrar como Cliente)
```

La configuración `demo-local` de `angular.json` reemplaza **solo** `core/auth.providers.ts` por
`core/auth.providers.demo-local.ts` (y `environment.ts` por `environment.demo-local.ts`): la sesión la entrega el IDaaS
local de `demo-local/idaas-local.mjs` a través del proxy (`proxy.demo-local.json`, ruta `/idaas`) y un interceptor
adjunta el token a `/api/**`. Las vistas, guards de rol y llamadas son las mismas, y los microservicios siguen validando
firma, emisor, audiencia, rol y scope. La barra muestra la etiqueta «Demo local». `ng build` y `npm start` nunca
incluyen este código. Los pasos completos, con el backend, están en `../demo-local/LEEME.md`.

### Modo local completo (Keycloak + Kong en Docker)

```bash
npm run local-completo   # = ng serve --configuration local-completo · http://localhost:4200
```

Reemplaza los dos servicios de nube por sus equivalentes en Docker (`backend-pedidos360/docker-compose.nube-local.yml`):
el login es real en **Keycloak** (realm `pedidos360`, en lugar de Entra External ID) con OpenID Connect, Authorization
Code + PKCE S256 (`oidc-client-ts` en lugar de MSAL), y todas las llamadas van a **Kong** (`http://localhost:8000`, en
lugar de AWS API Gateway), que valida el JWT y tiene CORS para `http://localhost:4200`. `angular.json` reemplaza
`core/auth.providers.ts` por `core/auth.providers.local-completo.ts` (servicio, guard e interceptor en
`core/local-completo/`) y `environment.ts` por `environment.local-completo.ts`. La barra muestra «Local: Keycloak + Kong».
Usuarios: `admin` / `Admin.360` y `cliente` / `Cliente.360`. Pasos con el backend: sección 8 de `../demo-local/LEEME.md`.
`ng build` de producción no incluye este código (se comprobó en el bundle).

## Vistas de EP5 (Kafka)

| Ruta | Rol | Qué muestra | Backend |
|---|---|---|---|
| `/pedidos` | Cliente/Admin | Un pedido nuevo aparece en CREADO y la vista vuelve a consultar cada 2 s (hasta 10 veces, unos 20 s) hasta que el inventario (vía Kafka) lo pasa a EN_PREPARACION o CANCELADO, con el motivo | ms-pedidos |
| `/productos` | Admin | Botón *Reponer 10 unidades* (`PATCH /api/productos/{id}/stock`), que publica STOCK_REPUESTO en Kafka | ms-productos |
| `/analitica` | Admin | Métricas en tiempo cuasi real (consulta cada 3 s): pedidos por estado, ventas, alertas de stock bajo y los últimos eventos con su tópico, partición, offset y clave | ms-analitica (`/api/analitica/resumen`) |
| `/kafka` | Admin | Brokers y controlador; tópicos con particiones, líder de cada partición, ISR, retención y mensajes; consumer groups con lag por partición; crear tópico, cambiar retención y eliminar | ms-kafka-admin (`/api/kafka/**`) |
