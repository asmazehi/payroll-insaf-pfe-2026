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

  coreNav: NavItem[] = [
    { label: 'nav.overview',  icon: 'grid_view',     route: '/dashboard' },
    { label: 'nav.reports',   icon: 'bar_chart',     route: '/reports' },
    { label: 'nav.forecast',  icon: 'trending_up',   route: '/forecast',  roles: ['ROLE_ADMIN'] },
    { label: 'nav.anomalies', icon: 'warning_amber', route: '/anomalies', roles: ['ROLE_ADMIN'] },
    { label: 'nav.assistant', icon: 'smart_toy',     route: '/chatbot' },
  ];

  pipelineNav: NavItem[] = [
    { label: 'nav.ingest', icon: 'cloud_upload',  route: '/ingest', roles: ['ROLE_ADMIN'] },
  ];

  adminNav: NavItem[] = [
    { label: 'nav.users', icon: 'manage_accounts', route: '/users', roles: ['ROLE_ADMIN'] },
  ];

  constructor(public auth: AuthService) {}

  isVisible(item: NavItem): boolean {
    if (!item.roles) return true;
    const role = this.auth.getCurrentUser()?.role || '';
    return item.roles.includes(role);
  }
}
