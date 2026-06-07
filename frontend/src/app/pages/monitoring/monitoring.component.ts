import { Component } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { environment } from '../../../environments/environment';

@Component({
  selector: 'app-monitoring',
  templateUrl: './monitoring.component.html',
  styleUrls: ['./monitoring.component.scss']
})
export class MonitoringComponent {
  readonly GRAFANA_BASE = environment.production ? '/grafana' : 'http://localhost:3000';

  tabs = [
    {
      label: 'Containers',
      icon: 'memory',
      url: `${this.GRAFANA_BASE}/d/1e64d971-f906-44c1-a51e-6bd1ce90de3a/insaf-platform-ae282ac-e2809d-containers?orgId=1&refresh=10s&kiosk`
    },
    {
      label: 'Prometheus',
      icon: 'analytics',
      url: `${this.GRAFANA_BASE}/explore?orgId=1&kiosk`
    }
  ];

  activeTab = 0;
  safeUrls: SafeResourceUrl[];

  constructor(private sanitizer: DomSanitizer) {
    this.safeUrls = this.tabs.map(t =>
      this.sanitizer.bypassSecurityTrustResourceUrl(t.url)
    );
  }

  setTab(i: number): void {
    this.activeTab = i;
  }
}
