import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, Router } from '@angular/router';
import { AuthService } from '../services/auth.service';

@Injectable({ providedIn: 'root' })
export class RoleGuard implements CanActivate {
  constructor(private auth: AuthService, private router: Router) {}

  canActivate(route: ActivatedRouteSnapshot): boolean {
    const required: string[] = route.data['roles'] || [];
    const userRole = this.auth.getCurrentUser()?.role || '';
    if (!required.length || required.includes(userRole)) return true;
    this.router.navigate(['/dashboard']);
    return false;
  }
}
