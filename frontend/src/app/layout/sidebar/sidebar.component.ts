import { Component } from '@angular/core';
import { AuthService } from '../../services/auth.service';

interface NavItem {
  label: string;
  icon: string;
  route: string;
  roles?: string[];
}

@Component({
  selector: 'app-sidebar',
  templateUrl: './sidebar.component.html',
  styleUrls: ['./sidebar.component.scss']
})
export class SidebarComponent {
  nav: NavItem[] = [
    { label: 'Dashboard',   icon: 'dashboard',    route: '/dashboard' },
    { label: 'Forecast',    icon: 'trending_up',  route: '/forecast',  roles: ['ROLE_ADMIN'] },
    { label: 'Anomalies',   icon: 'warning_amber', route: '/anomalies', roles: ['ROLE_ADMIN'] },
    { label: 'Reports',     icon: 'bar_chart',    route: '/reports' },
    { label: 'AI Assistant',icon: 'smart_toy',    route: '/chatbot' },
    { label: 'Data Ingest', icon: 'cloud_upload', route: '/ingest',  roles: ['ROLE_ADMIN'] },
  ];

  constructor(public auth: AuthService) {}

  get visibleNav(): NavItem[] {
    const role = this.auth.getCurrentUser()?.role || '';
    return this.nav.filter(n => !n.roles || n.roles.includes(role));
  }
}
