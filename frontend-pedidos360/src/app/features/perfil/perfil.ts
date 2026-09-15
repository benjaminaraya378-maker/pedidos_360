import { Component, computed, inject, signal } from '@angular/core';
import { AuthService } from '../../core/auth.service';
import { claimAsString } from '../../core/jwt.util';

/**
 * Muestra los claims del ID token y del access token emitidos por el IDaaS:
 * issuer, audience, expiración, roles (app roles) y scopes (scp).
 */
@Component({
  selector: 'app-perfil',
  imports: [],
  templateUrl: './perfil.html',
  styleUrl: './perfil.scss',
})
export class Perfil {
  protected readonly auth = inject(AuthService);
  protected readonly aviso = signal<string | null>(null);
  protected readonly ocupado = signal(false);

  protected readonly claimsId = computed(() => this.entradas(this.auth.idTokenClaims()));
  protected readonly claimsAccess = computed(() => this.entradas(this.auth.accessClaims()));

  protected readonly claimsDestacados = computed(() => {
    const c = this.auth.accessClaims();
    return [
      { nombre: 'iss (emisor)', valor: claimAsString(c, 'iss') },
      { nombre: 'aud (audience)', valor: claimAsString(c, 'aud') },
      { nombre: 'exp (expira)', valor: this.auth.tokenExpira() },
      { nombre: 'roles', valor: this.auth.roles().join(', ') || '— (sin rol asignado: se trata como Cliente)' },
      { nombre: 'scp (scopes)', valor: this.auth.scopes().join(' ') },
      { nombre: 'oid (id de usuario)', valor: claimAsString(c, 'oid') || claimAsString(c, 'sub') },
    ];
  });

  private entradas(obj: Record<string, unknown> | null): { clave: string; valor: string }[] {
    if (!obj) return [];
    return Object.entries(obj).map(([clave, valor]) => ({
      clave,
      valor: typeof valor === 'object' ? JSON.stringify(valor) : String(valor),
    }));
  }

  renovarToken(): void {
    this.ocupado.set(true);
    this.auth.refreshAccessToken(undefined, true).subscribe({
      next: () => {
        this.aviso.set('Access token renovado con acquireTokenSilent.');
        this.ocupado.set(false);
      },
      error: (err) => {
        this.aviso.set('No se pudo renovar: ' + String(err));
        this.ocupado.set(false);
      },
    });
  }

  async copiarToken(): Promise<void> {
    const token = this.auth.accessToken();
    if (!token) return;
    await navigator.clipboard.writeText(token);
    this.aviso.set('Access token copiado (puedes decodificarlo en https://jwt.ms).');
  }
}
