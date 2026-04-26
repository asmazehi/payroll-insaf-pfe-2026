import { Component } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';
import { AuthService } from '../../services/auth.service';
import { filter } from 'rxjs/operators';

const TITLES: Record<string, string> = {
  '/dashboard': 'Dashboard',
  '/forecast':  'Payroll Forecast',
  '/anomalies': 'Anomaly Detection',
  '/reports':   'Reports & Analytics',
  '/chatbot':   'AI Assistant',
};

@Component({
  selector: 'app-navbar',
  templateUrl: './navbar.component.html',
  styleUrls: ['./navbar.component.scss']
})
export class NavbarComponent {
  user = this.auth.getCurrentUser();
  pageTitle = 'Dashboard';

  constructor(private auth: AuthService, private router: Router) {
    this.router.events.pipe(filter(e => e instanceof NavigationEnd)).subscribe((e: any) => {
      this.pageTitle = TITLES[e.urlAfterRedirects] || 'INSAF';
    });
    this.pageTitle = TITLES[this.router.url] || 'INSAF';
  }

  logout(): void { this.auth.logout(); }
}
