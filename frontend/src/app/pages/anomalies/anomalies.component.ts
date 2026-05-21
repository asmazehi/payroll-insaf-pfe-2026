import { Component, OnInit } from '@angular/core';
import { forkJoin } from 'rxjs';
import { MlService } from '../../services/ml.service';

@Component({
  selector: 'app-anomalies',
  templateUrl: './anomalies.component.html',
  styleUrls: ['./anomalies.component.scss']
})
export class AnomaliesComponent implements OnInit {
  loading          = true;
  loadingBreakdown = true;
  result: any      = null;
  error            = '';

  activeTab: 0 | 1 | 2 = 0;
  filter: 'all' | 'high' | 'medium' | 'low' = 'all';
  selected: any    = null;

  ministryData: any[] = [];
  gradeData:    any[] = [];

  constructor(private ml: MlService) {}

  ngOnInit(): void {
    this.ml.getAnomalies(300).subscribe({
      next:  (res: any) => { this.result = res; this.loading = false; },
      error: () => {
        this.error   = 'ML service unavailable. Please start the Python API.';
        this.loading = false;
      },
    });

    forkJoin([
      this.ml.getAnomaliesByMinistry(),
      this.ml.getAnomaliesByGrade(),
    ]).subscribe({
      next: ([ministries, grades]: any) => {
        this.ministryData     = ministries as any[];
        this.gradeData        = grades    as any[];
        this.loadingBreakdown = false;
      },
      error: () => { this.loadingBreakdown = false; },
    });
  }

  // ── KPI counts — from full dataset (severity_summary), not the fetched subset ──

  get totalAnomalies():        number { return this.result?.total_anomalies_in_report  ?? 0; }
  get countHigh():             number { return this.result?.severity_summary?.high     ?? 0; }
  get countMedium():           number { return this.result?.severity_summary?.medium   ?? 0; }
  get countLow():              number { return this.result?.severity_summary?.low      ?? 0; }
  get totalSampledEmployees(): number { return this.result?.total_sampled_employees    ?? 0; }
  get totalSampledRecords():   number { return this.result?.total_sampled_records      ?? 0; }
  get anomalyRate(): string {
    const r = this.result?.anomaly_rate_pct;
    return r != null ? r.toFixed(2) + '%' : '—';
  }

  // Whether the CSV has been retrained with the new columns
  get hasNewCols(): boolean { return this.result?.has_new_cols === true; }

  // Whether any Low-severity records exist in the full dataset
  get hasLow(): boolean { return this.countLow > 0; }

  // ── Records tab ──────────────────────────────────────────────────

  get filtered(): any[] {
    const rows: any[] = this.result?.anomalies || [];
    if (this.filter === 'high')   return rows.filter(r => this.severity(r) === 'high');
    if (this.filter === 'medium') return rows.filter(r => this.severity(r) === 'medium');
    if (this.filter === 'low')    return rows.filter(r => this.severity(r) === 'low');
    return rows;
  }

  // Count in the currently DISPLAYED set (used by filter pills)
  countInView(level: string): number {
    return (this.result?.anomalies || []).filter((r: any) => this.severity(r) === level).length;
  }

  severity(r: any): 'high' | 'medium' | 'low' {
    const z = Math.abs(r.z_score ?? r.anomaly_score ?? 0);
    if (z >= 3.5) return 'high';
    if (z >= 2.5) return 'medium';
    return 'low';
  }

  severityLabel(r: any): string { return this.severity(r); }

  // ── Detail modal ─────────────────────────────────────────────────

  temporalLoading = false;

  openDetail(row: any): void {
    this.selected = { ...row };
    // Always fetch live temporal context from DW (CSV values may be null due to sampling)
    if (row.employee_sk && row.year_num && row.month_num) {
      this.temporalLoading = true;
      this.ml.getAnomalyTemporalContext(row.employee_sk, row.year_num, row.month_num)
        .subscribe({
          next: (ctx: any) => {
            this.selected = { ...this.selected, ...ctx };
            this.temporalLoading = false;
          },
          error: () => { this.temporalLoading = false; },
        });
    }
  }

  closeDetail(): void { this.selected = null; }

  hasTemporalContext(r: any): boolean {
    return r && (r.pay_prev_1m != null || r.pay_next_1m != null || r.pay_current != null);
  }

  fmtPct(n: number | null | undefined): string {
    if (n == null || isNaN(n)) return '—';
    const sign = n > 0 ? '+' : '';
    return `${sign}${n.toFixed(1)}%`;
  }

  // ── Shared utils ─────────────────────────────────────────────────

  abs(n: number): number { return Math.abs(n); }

  fmt(n: number | null | undefined): string {
    if (n == null || isNaN(+n)) return '—';
    return Number(n).toLocaleString('fr-TN', { maximumFractionDigits: 0 }) + ' TND';
  }

  monthLabel(yearNum: number, monthNum: number, offset: number): string {
    if (!yearNum || !monthNum) return '—';
    const d = new Date(yearNum, monthNum - 1 + offset, 1);
    return d.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
  }
}
