/**
 * Configuración de PRODUCCIÓN (se usa con `ng build`).
 * Reemplaza los valores "REEMPLAZAR-*" con los datos de tu tenant de
 * Microsoft Entra External ID y de tu API Gateway (ver docs/01-azure-entra-external-id.md
 * y docs/02-aws-api-gateway.md).
 */
export const environment = {
  production: true,

  msal: {
    // Application (client) ID de la app registration "Pedidos360-Web" (SPA)
    clientId: 'REEMPLAZAR-CLIENT-ID-SPA',

    // Entra External ID:  https://<subdominio>.ciamlogin.com/<tenant-id>/
    // Azure AD B2C (legado): https://<tenant>.b2clogin.com/<tenant>.onmicrosoft.com/B2C_1_signupsignin
    authority: 'https://REEMPLAZAR-SUBDOMINIO.ciamlogin.com/REEMPLAZAR-TENANT-ID/',

    // Dominio del IDaaS que MSAL aceptará como emisor (sin https://)
    knownAuthorities: ['REEMPLAZAR-SUBDOMINIO.ciamlogin.com'],

    // URL pública del frontend (CloudFront) — debe estar registrada como Redirect URI tipo SPA
    redirectUri: 'https://REEMPLAZAR-DISTRIBUCION.cloudfront.net',
    postLogoutRedirectUri: 'https://REEMPLAZAR-DISTRIBUCION.cloudfront.net',
  },

  api: {
    // URL de invocación del API Gateway (HTTP API, stage $default)
    baseUrl: 'https://REEMPLAZAR-API-ID.execute-api.us-east-1.amazonaws.com',

    // Scopes expuestos por la app registration "Pedidos360-API"
    scopes: [
      'api://REEMPLAZAR-CLIENT-ID-API/Pedidos360.Read',
      'api://REEMPLAZAR-CLIENT-ID-API/Pedidos360.Write',
    ],
  },
};
