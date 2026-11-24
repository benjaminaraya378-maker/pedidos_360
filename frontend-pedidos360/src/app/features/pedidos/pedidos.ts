import { Component, DestroyRef, inject, signal } from '@angular/core';
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

  private readonly destroyRef = inject(DestroyRef);
  private seguimiento?: ReturnType<typeof setTimeout>;
  private consultasDeSeguimiento = 0;

  constructor() {
    this.cargar();
    this.destroyRef.onDestroy(() => clearTimeout(this.seguimiento));
  }

  cargar(): void {
    this.cargando.set(true);
    this.error.set(null);
    this.api.listarPedidos().subscribe({
      next: (lista) => {
        this.pedidos.set(lista);
        this.cargando.set(false);
        this.seguirPendientes();
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
        this.mensaje.set(
          `Pedido #${pedido.id} creado por ${pedido.total.toLocaleString('es-CL')} CLP. ` +
            'El inventario lo confirmará en unos segundos.',
        );
        this.carrito.vaciar();
        this.enviando.set(false);
        this.consultasDeSeguimiento = 0;
        this.cargar();
      },
      error: (err) => {
        this.error.set(ApiService.mensajeDeError(err));
        this.enviando.set(false);
      },
    });
  }

  /**
   * Un pedido en CREADO está esperando al inventario: ms-productos descuenta el stock al leer el evento de Kafka y
   * ms-pedidos lo pasa a EN_PREPARACION o CANCELADO. Mientras haya alguno así, se vuelve a consultar cada 2 s
   * (máximo 10 veces) para que el cambio se vea sin recargar la página.
   */
  private seguirPendientes(): void {
    clearTimeout(this.seguimiento);
    if (this.consultasDeSeguimiento >= 10 || !this.pedidos().some((p) => p.estado === 'CREADO')) return;
    this.consultasDeSeguimiento++;
    this.seguimiento = setTimeout(() => {
      this.api.listarPedidos().subscribe({
        next: (lista) => {
          this.pedidos.set(lista);
          this.seguirPendientes();
        },
        error: () => undefined,
      });
    }, 2000);
  }

  cantidadDesdeEvento(ev: Event): number {
    return Number((ev.target as HTMLInputElement).value);
  }
}
