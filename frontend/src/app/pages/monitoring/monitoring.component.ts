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

  readonly dashboardUrl = `${this.GRAFANA_BASE}/d/de697fd8-5441-4233-a24e-ffd6e9af7883/insaf-platform-ae282ac-e2809d-containers?orgId=1&refresh=10s&kiosk`;

  safeUrl: SafeResourceUrl;

  constructor(private sanitizer: DomSanitizer) {
    this.safeUrl = this.sanitizer.bypassSecurityTrustResourceUrl(this.dashboardUrl);
  }
}
