/**
 * Configuración del MODO DEMO LOCAL (`npm run demo-local` = `ng serve --configuration demo-local`).
 * Igual que la de desarrollo, pero la sesión la emite el IDaaS local (demo-local/idaas-local.mjs) y no
 * Entra External ID: core/auth.providers.demo-local.ts reemplaza a MSAL. Los valores de `msal` no se usan.
 * El dev-server reenvía /api/** a los microservicios y /idaas/** al IDaaS local (proxy.demo-local.json).
 */
export const environment = {
  production: false,

  msal: {
    clientId: 'no-se-usa-en-demo-local',
    authority: 'https://login.invalid/',
    knownAuthorities: ['login.invalid'],
    redirectUri: 'http://localhost:4200',
    postLogoutRedirectUri: 'http://localhost:4200',
  },

  api: {
    baseUrl: 'http://localhost:4200',
    scopes: ['Pedidos360.Read', 'Pedidos360.Write'],
  },
};
