import { Component, inject, signal } from '@angular/core';
import { CurrencyPipe, DatePipe } from '@angular/common';
import { RouterLink } from '@angular/router';
import { ApiService } from '../../core/api.service';
import { AuthService } from '../../core/auth.service';
import { CarritoService } from '../../core/carrito.service';
import { Pedido } from '../../core/models';

@Component({
  selector: 'app-pedidos',
  imports: [CurrencyPipe, DatePipe, RouterLink],
  templateUrl: './pedidos.html',
  styleUrl: './pedidos.scss',
})
export class Pedidos {
  protected readonly api = inject(ApiService);
  protected readonly auth = inject(AuthService);
  protected readonly carrito = inject(CarritoService);

  protected readonly pedidos = signal<Pedido[]>([]);
  protected readonly cargando = signal(true);
  protected readonly enviando = signal(false);
  protected readonly error = signal<string | null>(null);
  protected readonly mensaje = signal<string | null>(null);

  constructor() {
    this.cargar();
  }

  cargar(): void {
    this.cargando.set(true);
    this.error.set(null);
    this.api.listarPedidos().subscribe({
      next: (lista) => {
        this.pedidos.set(lista);
        this.cargando.set(false);
      },
      error: (err) => {
        this.error.set(ApiService.mensajeDeError(err));
        this.cargando.set(false);
      },
    });
  }

  confirmar(): void {
    const items = this.carrito.lineas().map((l) => ({ productoId: l.producto.id, cantidad: l.cantidad }));
    if (items.length === 0) return;
    this.enviando.set(true);
    this.error.set(null);
    this.api.crearPedido({ items }).subscribe({
      next: (pedido) => {
        this.mensaje.set(`Pedido #${pedido.id} creado por ${pedido.total.toLocaleString('es-CL')} CLP.`);
        this.carrito.vaciar();
        this.enviando.set(false);
        this.cargar();
      },
      error: (err) => {
        this.error.set(ApiService.mensajeDeError(err));
        this.enviando.set(false);
      },
    });
  }

  cantidadDesdeEvento(ev: Event): number {
    return Number((ev.target as HTMLInputElement).value);
  }
}
