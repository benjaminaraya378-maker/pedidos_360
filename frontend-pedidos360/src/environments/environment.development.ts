/**
 * Configuración de DESARROLLO LOCAL (se usa con `ng serve`).
 * El frontend habla siempre con rutas /api/** ; en local, proxy.conf.json las
 * redirige a los microservicios que corren en tu máquina (8080/8081/8082),
 * imitando exactamente la forma de las rutas del API Gateway.
 */
export const environment = {
  production: false,

  msal: {
    clientId: 'REEMPLAZAR-CLIENT-ID-SPA',
    authority: 'https://REEMPLAZAR-SUBDOMINIO.ciamlogin.com/REEMPLAZAR-TENANT-ID/',
    knownAuthorities: ['REEMPLAZAR-SUBDOMINIO.ciamlogin.com'],
    redirectUri: 'http://localhost:4200',
    postLogoutRedirectUri: 'http://localhost:4200',
  },

  api: {
    // En local el dev-server de Angular (puerto 4200) hace de "API Manager" con proxy.conf.json.
    // Si quieres probar contra el API Gateway real desde local, pon aquí su URL.
    baseUrl: 'http://localhost:4200',
    scopes: [
      'api://REEMPLAZAR-CLIENT-ID-API/Pedidos360.Read',
      'api://REEMPLAZAR-CLIENT-ID-API/Pedidos360.Write',
    ],
  },
};
