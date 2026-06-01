import { Component, OnInit, OnDestroy } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';
import { AuthService } from '../../services/auth.service';
import { AdminService } from '../../services/admin.service';
import { LangService, Lang } from '../../services/lang.service';
import { filter } from 'rxjs/operators';
import { Subscription, interval } from 'rxjs';

const TITLES: Record<string, string> = {
  '/dashboard': 'Dashboard',
  '/forecast':  'Payroll Forecast',
  '/anomalies': 'Anomaly Detection',
  '/reports':   'Reports & Analytics',
  '/chatbot':   'AI Assistant',
  '/tickets':   'Support Tickets',
  '/profile':   'My Profile',
};

@Component({
  selector: 'app-navbar',
  templateUrl: './navbar.component.html',
  styleUrls: ['./navbar.component.scss']
})
export class NavbarComponent implements OnInit, OnDestroy {
  user = this.auth.getCurrentUser();
  pageTitle = 'Dashboard';
  openTicketCount = 0;
  private pollSub?: Subscription;

  constructor(
    public auth: AuthService,
    private adminService: AdminService,
    private router: Router,
    public lang: LangService
  ) {
    this.router.events.pipe(filter(e => e instanceof NavigationEnd)).subscribe((e: any) => {
      this.pageTitle = TITLES[e.urlAfterRedirects] || 'INSAF';
    });
    this.pageTitle = TITLES[this.router.url] || 'INSAF';
  }

  ngOnInit(): void {
    if (this.auth.isAdmin()) {
      this.refreshCount();
      this.pollSub = interval(30000).subscribe(() => this.refreshCount());
    }
  }

  ngOnDestroy(): void { this.pollSub?.unsubscribe(); }

  refreshCount(): void {
    this.adminService.getOpenTicketCount().subscribe({
      next: (r) => this.openTicketCount = r.count,
      error: () => {}
    });
  }

  goToTickets(): void { this.router.navigate(['/tickets']); }

  setLang(code: Lang): void { this.lang.setLang(code); }
  logout(): void { this.auth.logout(); }
}
