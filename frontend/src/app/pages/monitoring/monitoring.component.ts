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
  readonly PROMETHEUS_BASE = environment.production ? '/prometheus' : 'http://localhost:9090';

  tabs = [
    {
      labelKey: 'monitoring.containers',
      icon: 'memory',
      url: `${this.GRAFANA_BASE}/d/de697fd8-5441-4233-a24e-ffd6e9af7883/insaf-platform-ae282ac-e2809d-containers?orgId=1&refresh=10s&kiosk`
    },
    {
      labelKey: 'monitoring.prometheus',
      icon: 'analytics',
      url: `${this.PROMETHEUS_BASE}/graph`
    }
  ];

  activeTab = 0;
  safeUrls: SafeResourceUrl[];

  constructor(private sanitizer: DomSanitizer) {
    this.safeUrls = this.tabs.map(t =>
      this.sanitizer.bypassSecurityTrustResourceUrl(t.url)
    );
  }

  setTab(i: number): void { this.activeTab = i; }
}
