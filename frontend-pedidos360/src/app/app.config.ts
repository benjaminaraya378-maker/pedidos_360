import {
  ApplicationConfig,
  LOCALE_ID,
  provideBrowserGlobalErrorListeners,
  provideZoneChangeDetection,
} from '@angular/core';
import { registerLocaleData } from '@angular/common';
import localeEsCl from '@angular/common/locales/es-CL';
import { provideRouter, withInMemoryScrolling } from '@angular/router';
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';

import { routes } from './app.routes';
import { authProviders } from './core/auth.providers';

// Montos y números con formato chileno ($129.990), igual que los textos que arma el backend.
registerLocaleData(localeEsCl);

export const appConfig: ApplicationConfig = {
  providers: [
    { provide: LOCALE_ID, useValue: 'es-CL' },
    provideBrowserGlobalErrorListeners(),
    provideZoneChangeDetection({ eventCoalescing: true }),
    provideRouter(routes, withInMemoryScrolling({ scrollPositionRestoration: 'top' })),
    provideHttpClient(withInterceptorsFromDi()),

    // MSAL + Entra External ID (core/auth.providers.ts). En `ng serve --configuration demo-local`
    // angular.json lo reemplaza por el IDaaS local de demostración (core/auth.providers.demo-local.ts).
    ...authProviders,
  ],
};
