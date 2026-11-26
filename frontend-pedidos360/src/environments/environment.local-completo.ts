/**
 * Configuración del MODO LOCAL COMPLETO (`npm run local-completo` = `ng serve --configuration local-completo`).
 * Reemplaza la nube por contenedores Docker en este PC:
 *  - IDaaS: Keycloak (realm "pedidos360") en lugar de Microsoft Entra External ID. core/auth.providers.local-completo.ts
 *    reemplaza a MSAL por oidc-client-ts (Authorization Code + PKCE S256). Los valores de `msal` no se usan.
 *  - API Manager: Kong en lugar de AWS API Gateway. El frontend llama directo a Kong (:8000), que valida el JWT y
 *    reenvía /api/** a los microservicios; Kong tiene CORS para http://localhost:4200, así que no hay proxy.
 * Cómo levantar Keycloak y Kong: ../demo-local/LEEME.md.
 */
export const environment = {
  production: false,

  msal: {
    clientId: 'no-se-usa-en-local-completo',
    authority: 'https://login.invalid/',
    knownAuthorities: ['login.invalid'],
    redirectUri: 'http://localhost:4200',
    postLogoutRedirectUri: 'http://localhost:4200',
  },

  api: {
    // Proxy de Kong (el "API Gateway" local)
    baseUrl: 'http://localhost:8000',
    scopes: ['Pedidos360.Read', 'Pedidos360.Write'],
  },

  oidc: {
    // Emisor (iss) de los tokens; el discovery está en <authority>/.well-known/openid-configuration
    authority: 'http://localhost:8180/realms/pedidos360',
    // Cliente público del realm (sin secreto), con Authorization Code + PKCE
    clientId: 'pedidos360-web',
    scope: 'openid profile email',
  },
};
