import { Component, inject, signal } from '@angular/core';
import { DecimalPipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Observable, forkJoin, of, switchMap } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { ClusterKafka, GrupoDetalle, GrupoResumen, TopicoDetalle } from '../../core/models';

const DIA_MS = 86_400_000;

/**
 * Administración de Kafka (rol Admin) sobre ms-kafka-admin: brokers, tópicos con su configuración y la ubicación de
 * cada partición, consumer groups con su lag, y operaciones para crear tópicos, cambiar la retención y eliminar.
 */
@Component({
  selector: 'app-kafka',
  imports: [FormsModule, DecimalPipe],
  templateUrl: './kafka.html',
  styleUrl: './kafka.scss',
})
export class Kafka {
  private readonly api = inject(ApiService);

  protected readonly cluster = signal<ClusterKafka | null>(null);
  protected readonly topicos = signal<TopicoDetalle[]>([]);
  protected readonly grupos = signal<GrupoResumen[]>([]);
  protected readonly grupoAbierto = signal<GrupoDetalle | null>(null);
  protected readonly cargando = signal(true);
  protected readonly ocupado = signal(false);
  protected readonly error = signal<string | null>(null);
  protected readonly mensaje = signal<string | null>(null);

  protected retencionTopico = '';
  protected retencionDias = 7;
  protected nuevo = { nombre: '', particiones: 3, replicas: 3, cleanupPolicy: 'delete', retencionDias: 7 };

  constructor() {
    this.cargar();
  }

  cargar(): void {
    this.cargando.set(true);
    this.error.set(null);
    forkJoin({
      cluster: this.api.kafkaCluster(),
      topicos: this.api
        .kafkaTopicos()
        .pipe(switchMap((lista) => (lista.length ? forkJoin(lista.map((t) => this.api.kafkaTopico(t.nombre))) : of([])))),
      grupos: this.api.kafkaGrupos(),
    }).subscribe({
      next: ({ cluster, topicos, grupos }) => {
        this.cluster.set(cluster);
        this.topicos.set(topicos);
        this.grupos.set(grupos);
        if (!topicos.some((t) => t.nombre === this.retencionTopico)) this.retencionTopico = topicos[0]?.nombre ?? '';
        this.cargando.set(false);
        const abierto = this.grupoAbierto();
        if (abierto) this.verGrupo(abierto.grupo);
      },
      error: (err) => {
        this.error.set(ApiService.mensajeDeError(err));
        this.cargando.set(false);
      },
    });
  }

  config(t: TopicoDetalle, clave: string): string {
    return t.configuracion[clave]?.valor ?? '—';
  }

  /** Retención legible: compactado, sin límite o en días. */
  retencion(t: TopicoDetalle): string {
    const politica = this.config(t, 'cleanup.policy');
    if (politica === 'compact') return 'compactado (último por clave)';
    const ms = Number(this.config(t, 'retention.ms'));
    if (ms < 0) return 'sin límite';
    const dias = ms / DIA_MS;
    return dias >= 1 ? `${+dias.toFixed(2)} día(s)` : `${Math.round(ms / 3_600_000)} h`;
  }

  lideres(t: TopicoDetalle): string {
    return t.detalleParticiones.map((p) => `p${p.particion}→b${p.lider ?? '?'}`).join('  ');
  }

  isrCompleto(t: TopicoDetalle): boolean {
    return t.detalleParticiones.every((p) => p.isr.length === p.replicas.length);
  }

  verGrupo(grupo: string): void {
    this.api.kafkaGrupo(grupo).subscribe({
      next: (detalle) => this.grupoAbierto.set(detalle),
      error: (err) => this.error.set(ApiService.mensajeDeError(err)),
    });
  }

  cambiarRetencion(): void {
    const ms = String(Math.round(this.retencionDias * DIA_MS));
    this.ejecutar(
      this.api.configurarTopico(this.retencionTopico, { establecer: { 'retention.ms': ms }, restablecer: [] }),
      `Retención de ${this.retencionTopico} cambiada a ${this.retencionDias} día(s), sin reiniciar nada.`,
    );
  }

  crear(): void {
    const n = this.nuevo;
    const configuracion: Record<string, string> = {
      'cleanup.policy': n.cleanupPolicy,
      'min.insync.replicas': String(Math.min(2, n.replicas)),
    };
    if (n.cleanupPolicy !== 'compact') configuracion['retention.ms'] = String(Math.round(n.retencionDias * DIA_MS));
    this.ejecutar(
      this.api.crearTopico({ nombre: n.nombre.trim(), particiones: n.particiones, replicas: n.replicas, configuracion }),
      `Tópico ${n.nombre.trim()} creado con ${n.particiones} particiones y ${n.replicas} réplicas.`,
    );
  }

  eliminar(t: TopicoDetalle): void {
    if (!confirm(`¿Eliminar el tópico ${t.nombre}? Se pierden sus ${t.mensajes} mensajes.`)) return;
    this.ejecutar(this.api.eliminarTopico(t.nombre), `Tópico ${t.nombre} eliminado.`);
  }

  private ejecutar(operacion: Observable<unknown>, exito: string): void {
    this.ocupado.set(true);
    this.error.set(null);
    this.mensaje.set(null);
    operacion.subscribe({
      next: () => {
        this.mensaje.set(exito);
        this.ocupado.set(false);
        this.cargar();
      },
      error: (err) => {
        this.error.set(ApiService.mensajeDeError(err));
        this.ocupado.set(false);
      },
    });
  }
}
