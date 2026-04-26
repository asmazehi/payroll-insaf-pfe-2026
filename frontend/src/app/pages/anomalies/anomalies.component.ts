import { Component, OnInit } from '@angular/core';
import { MlService } from '../../services/ml.service';

@Component({
  selector: 'app-anomalies',
  templateUrl: './anomalies.component.html',
  styleUrls: ['./anomalies.component.scss']
})
export class AnomaliesComponent implements OnInit {
  loading = true;
  result: any = null;
  error = '';
  filter: 'all' | 'high' | 'medium' | 'low' = 'all';
  selected: any = null;

  constructor(private ml: MlService) {}

  ngOnInit(): void {
    this.ml.getAnomalies(200).subscribe({
      next: (res: any) => { this.result = res; this.loading = false; },
      error: () => { this.error = 'ML service unavailable. Please start the Python API.'; this.loading = false; }
    });
  }

  get filtered(): any[] {
    const rows: any[] = this.result?.anomalies || [];
    if (this.filter === 'high')   return rows.filter(r => this.severity(r) === 'high');
    if (this.filter === 'medium') return rows.filter(r => this.severity(r) === 'medium');
    if (this.filter === 'low')    return rows.filter(r => this.severity(r) === 'low');
    return rows;
  }

  severity(r: any): 'high' | 'medium' | 'low' {
    const z = Math.abs(r.z_score ?? r.anomaly_score ?? 0);
    if (z >= 3.5) return 'high';
    if (z >= 2.5) return 'medium';
    return 'low';
  }

  severityLabel(r: any): string {
    return this.severity(r);
  }

  count(level: string): number {
    return (this.result?.anomalies || []).filter((r: any) => this.severity(r) === level).length;
  }

  openDetail(row: any): void { this.selected = row; }
  closeDetail(): void { this.selected = null; }

  abs(n: number): number { return Math.abs(n); }

  fmt(n: number): string {
    if (!n) return '—';
    return Number(n).toLocaleString('fr-TN', { maximumFractionDigits: 0 }) + ' TND';
  }
}
