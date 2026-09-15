import { Injectable, computed, signal } from '@angular/core';
import { Producto } from './models';

export interface LineaCarrito {
  producto: Producto;
  cantidad: number;
}

/** Carrito en memoria (se vacía al recargar). El pedido real lo crea el BFF → ms-pedidos. */
@Injectable({ providedIn: 'root' })
export class CarritoService {
  readonly lineas = signal<LineaCarrito[]>([]);
  readonly cantidadTotal = computed(() => this.lineas().reduce((acc, l) => acc + l.cantidad, 0));
  readonly total = computed(() => this.lineas().reduce((acc, l) => acc + l.cantidad * l.producto.precio, 0));

  agregar(producto: Producto): void {
    this.lineas.update((lineas) => {
      const existente = lineas.find((l) => l.producto.id === producto.id);
      if (existente) {
        return lineas.map((l) =>
          l.producto.id === producto.id ? { ...l, cantidad: Math.min(l.cantidad + 1, producto.stock) } : l,
        );
      }
      return [...lineas, { producto, cantidad: 1 }];
    });
  }

  cambiarCantidad(productoId: number, cantidad: number): void {
    this.lineas.update((lineas) =>
      lineas
        .map((l) => (l.producto.id === productoId ? { ...l, cantidad } : l))
        .filter((l) => l.cantidad > 0),
    );
  }

  quitar(productoId: number): void {
    this.lineas.update((lineas) => lineas.filter((l) => l.producto.id !== productoId));
  }

  vaciar(): void {
    this.lineas.set([]);
  }
}
