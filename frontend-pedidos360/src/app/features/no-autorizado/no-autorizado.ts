import { Component, inject } from '@angular/core';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { AuthService } from '../../core/auth.service';

@Component({
  selector: 'app-no-autorizado',
  imports: [RouterLink],
  template: `
    <div class="vacio">
      <h1>Sin autorización</h1>
      <p>
        Tu sesión es válida, pero esta sección requiere el rol
        <strong>{{ rol }}</strong>. Tus roles actuales: {{ auth.roles().join(', ') || 'ninguno' }}.
      </p>
      <p class="silencio">Un administrador puede asignarte el rol en Entra: Aplicaciones empresariales → Pedidos360-API → Usuarios y grupos.</p>
      <a routerLink="/" class="boton boton--secundario">Volver al inicio</a>
    </div>
  `,
})
export class NoAutorizado {
  protected readonly auth = inject(AuthService);
  protected readonly rol = inject(ActivatedRoute).snapshot.queryParamMap.get('rol') ?? 'requerido';
}
