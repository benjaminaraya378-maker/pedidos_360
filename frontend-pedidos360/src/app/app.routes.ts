import { Routes } from '@angular/router';
import { MsalGuard } from '@azure/msal-angular';
import { roleGuard } from './core/role.guard';

export const routes: Routes = [
  { path: '', loadComponent: () => import('./features/home/home').then((m) => m.Home), title: 'Pedidos360' },
  {
    path: 'productos',
    canActivate: [MsalGuard],
    loadComponent: () => import('./features/productos/productos').then((m) => m.Productos),
    title: 'Catálogo · Pedidos360',
  },
  {
    path: 'pedidos',
    canActivate: [MsalGuard],
    loadComponent: () => import('./features/pedidos/pedidos').then((m) => m.Pedidos),
    title: 'Pedidos · Pedidos360',
  },
  {
    path: 'perfil',
    canActivate: [MsalGuard],
    loadComponent: () => import('./features/perfil/perfil').then((m) => m.Perfil),
    title: 'Mi sesión · Pedidos360',
  },
  {
    path: 'admin',
    canActivate: [MsalGuard, roleGuard('Admin')],
    loadComponent: () => import('./features/admin/admin').then((m) => m.Admin),
    title: 'Administración · Pedidos360',
  },
  {
    path: 'no-autorizado',
    loadComponent: () => import('./features/no-autorizado/no-autorizado').then((m) => m.NoAutorizado),
    title: 'Sin autorización · Pedidos360',
  },
  { path: '**', redirectTo: '' },
];
