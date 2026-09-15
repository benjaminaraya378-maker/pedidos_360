# Pedidos360 · Frontend (Angular + MSAL)

Inicia sesión contra el tenant de **Microsoft Entra** con OpenID Connect (Authorization Code + PKCE) usando
`@azure/msal-browser` / `@azure/msal-angular`, protege rutas con `MsalGuard`, adjunta el access token con
`MsalInterceptor` y consume el backend a través de **AWS API Gateway**.

```
src/app/
├─ app.config.ts        # PublicClientApplication, MSAL_GUARD_CONFIG, MSAL_INTERCEPTOR_CONFIG (protectedResourceMap)
├─ app.routes.ts        # /productos, /pedidos, /perfil (MsalGuard) · /admin (MsalGuard + roleGuard('Admin'))
├─ core/auth.service.ts # login/logout, acquireTokenSilent, roles y scopes leídos de los claims del access token
├─ core/api.service.ts  # llamadas al API Gateway (/api/productos, /api/pedidos, /api/bff/**)
└─ features/            # home (dashboard BFF), productos, pedidos, perfil (claims del token), admin, no-autorizado
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
npm start            # http://localhost:4200 (proxy.conf.json envía /api/** a los microservicios locales)
npm run build:prod   # dist/frontend-pedidos360/browser → publicar en S3 + CloudFront (o similar con HTTPS)
```
