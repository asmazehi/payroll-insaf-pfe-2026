import { Component } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';

@Component({
  selector: 'app-reports',
  templateUrl: './reports.component.html',
  styleUrls: ['./reports.component.scss']
})
export class ReportsComponent {
  /* Replace EMBED_URL with your actual Power BI embed URL */
  readonly EMBED_URL = 'https://app.powerbi.com/reportEmbed?reportId=add0758b-df49-4f58-8f4c-f81b9fc9797c&autoAuth=true&ctid=604f1a96-cbe8-43f8-abbf-f8eaf5d85730&pageName=401078d09086ea36810e&filterPaneEnabled=false&navContentPaneEnabled=false';

  safeUrl: SafeResourceUrl;

  constructor(private sanitizer: DomSanitizer) {
    this.safeUrl = this.sanitizer.bypassSecurityTrustResourceUrl(this.EMBED_URL);
  }
}
