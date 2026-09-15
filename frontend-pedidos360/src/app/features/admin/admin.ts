import { Component, inject, signal } from '@angular/core';
import { CurrencyPipe, DatePipe } from '@angular/common';
import { ApiService } from '../../core/api.service';
import { Pedido } from '../../core/models';

/** Ruta protegida por rol Admin (guard en el frontend + hasRole('ADMIN') en el BFF). */
@Component({
  selector: 'app-admin',
  imports: [CurrencyPipe, DatePipe],
  templateUrl: './admin.html',
})
export class Admin {
  private readonly api = inject(ApiService);
  protected readonly pedidos = signal<Pedido[] | null>(null);
  protected readonly error = signal<string | null>(null);

  constructor() {
    this.api.pedidosAdmin().subscribe({
      next: (lista) => this.pedidos.set(lista),
      error: (err) => this.error.set(ApiService.mensajeDeError(err)),
    });
  }
}
