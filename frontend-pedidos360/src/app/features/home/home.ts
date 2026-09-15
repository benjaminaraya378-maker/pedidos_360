import { Component, effect, inject, signal } from '@angular/core';
import { CurrencyPipe, DatePipe } from '@angular/common';
import { RouterLink } from '@angular/router';
import { ApiService } from '../../core/api.service';
import { AuthService } from '../../core/auth.service';
import { Dashboard } from '../../core/models';

@Component({
  selector: 'app-home',
  imports: [RouterLink, CurrencyPipe, DatePipe],
  templateUrl: './home.html',
  styleUrl: './home.scss',
})
export class Home {
  protected readonly auth = inject(AuthService);
  private readonly api = inject(ApiService);
  protected readonly dashboard = signal<Dashboard | null>(null);
  protected readonly error = signal<string | null>(null);

  constructor() {
    // Con sesión iniciada, pide al BFF (vía API Gateway) el resumen del usuario.
    effect(() => {
      if (this.auth.isLoggedIn() && !this.auth.interactionInProgress() && !this.dashboard()) {
        this.api.dashboard().subscribe({
          next: (d) => this.dashboard.set(d),
          error: (err) => this.error.set(ApiService.mensajeDeError(err)),
        });
      }
    });
  }
}
