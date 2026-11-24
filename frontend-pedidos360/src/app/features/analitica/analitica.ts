import { Component, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { CurrencyPipe, DatePipe, DecimalPipe, PercentPipe } from '@angular/common';
import { EMPTY, catchError, filter, interval, startWith, switchMap } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { ResumenAnalitica } from '../../core/models';

/**
 * Analítica en tiempo cuasi real (rol Admin). ms-analitica mantiene las métricas actualizadas con cada evento de
 * Kafka; esta vista consulta GET /api/analitica/resumen cada 3 segundos, así un pedido nuevo, su confirmación o una
 * alerta de stock bajo aparecen en pantalla pocos segundos después de ocurrir.
 */
@Component({
  selector: 'app-analitica',
  imports: [CurrencyPipe, DatePipe, DecimalPipe, PercentPipe],
  templateUrl: './analitica.html',
  styleUrl: './analitica.scss',
})
export class Analitica {
  private readonly api = inject(ApiService);

  protected readonly resumen = signal<ResumenAnalitica | null>(null);
  protected readonly error = signal<string | null>(null);
  protected readonly enVivo = signal(true);
  protected readonly estados = ['CREADO', 'EN_PREPARACION', 'CANCELADO'];

  constructor() {
    interval(3000)
      .pipe(
        startWith(0),
        filter(() => this.enVivo()),
        switchMap(() =>
          this.api.resumenAnalitica().pipe(
            catchError((err) => {
              this.error.set(ApiService.mensajeDeError(err));
              return EMPTY;
            }),
          ),
        ),
        takeUntilDestroyed(),
      )
      .subscribe((r) => {
        this.resumen.set(r);
        this.error.set(null);
      });
  }

  alternar(): void {
    this.enVivo.update((v) => !v);
  }

  cantidad(estado: string): number {
    return this.resumen()?.pedidosPorEstado[estado] ?? 0;
  }

  /** "pedidos360.inventario" → "inventario" para la etiqueta del tópico. */
  corto(topico: string): string {
    return topico.replace('pedidos360.', '');
  }
}
