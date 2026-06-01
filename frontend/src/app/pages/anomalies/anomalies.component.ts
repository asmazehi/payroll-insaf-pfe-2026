import { Component, OnInit, OnDestroy } from '@angular/core';
import { forkJoin, Subscription } from 'rxjs';
import { MlService } from '../../services/ml.service';
import { LangService } from '../../services/lang.service';
import { TranslateService } from '@ngx-translate/core';

@Component({
  selector: 'app-anomalies',
  templateUrl: './anomalies.component.html',
  styleUrls: ['./anomalies.component.scss']
})
export class AnomaliesComponent implements OnInit, OnDestroy {
  loading          = true;
  loadingBreakdown = true;
  result: any      = null;
  error            = '';

  activeTab: 0 | 1 | 2 = 0;
  filter: 'all' | 'high' | 'medium' | 'low' = 'all';
  selected: any    = null;

  ministryData: any[] = [];
  gradeData:    any[] = [];
  private langSub?: Subscription;

  constructor(
    private ml: MlService,
    private lang: LangService,
    private translate: TranslateService
  ) {}

  ngOnInit(): void {
    this.loadAnomalies();
    this.loadBreakdown();

    // Reload diagnosis text when user switches language (use event.lang, not currentLang)
    this.langSub = this.translate.onLangChange.subscribe((event) => {
      this.loadAnomalies(event.lang);
    });
  }

  ngOnDestroy(): void { this.langSub?.unsubscribe(); }

  loadAnomalies(lang?: string): void {
    const l = lang || this.lang.current || 'en';
    this.ml.getAnomalies(300, l).subscribe({
      next:  (res: any) => { this.result = res; this.loading = false; },
      error: () => {
        this.error   = 'ML service unavailable. Please start the Python API.';
        this.loading = false;
      },
    });
  }

  loadBreakdown(): void {
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
  get unreviewed():            number { return this.result?.unreviewed                 ?? 0; }
  get reviewStats():           any    { return this.result?.review_stats               ?? {}; }
  get countHigh():             number { return this.result?.severity_summary?.high     ?? 0; }
  get countMedium():           number { return this.result?.severity_summary?.medium   ?? 0; }
  get countLow():              number { return this.result?.severity_summary?.low      ?? 0; }
  get totalSampledEmployees(): number { return this.result?.total_sampled_employees    ?? 0; }
  get totalSampledRecords():   number { return this.result?.total_sampled_records      ?? 0; }
  get anomalyRate(): string {
    const r = this.result?.anomaly_rate_pct;
    return r != null ? r.toFixed(2) + '%' : '—';
  }

  reviewing = false;
  reviewNotes = '';
  showMetrics = false;
  hideReviewed = false;
  reviewFilter: string | null = null;  // 'LEGITIMATE' | 'ERROR' | 'INVESTIGATING' | null

  submitReview(status: string): void {
    if (!this.selected) return;
    this.reviewing = true;
    this.ml.submitReview(
      this.selected.employee_sk,
      this.selected.year_num,
      this.selected.month_num,
      status,
      this.reviewNotes
    ).subscribe({
      next: () => {
        this.selected.review_status = status;
        this.reviewing = false;
        this.reviewNotes = '';
        this.loadAnomalies();
      },
      error: () => { this.reviewing = false; }
    });
  }

  clearReview(): void {
    if (!this.selected) return;
    this.ml.removeReview(this.selected.employee_sk, this.selected.year_num, this.selected.month_num)
      .subscribe({
        next: () => { this.selected.review_status = null; this.loadAnomalies(); },
        error: () => {}
      });
  }

  // Whether the CSV has been retrained with the new columns
  get hasNewCols(): boolean { return this.result?.has_new_cols === true; }

  // Whether any Low-severity records exist in the full dataset
  get hasLow(): boolean { return this.countLow > 0; }

  // ── Records tab ──────────────────────────────────────────────────

  get filtered(): any[] {
    let rows: any[] = this.result?.anomalies || [];
    if (this.reviewFilter)  return rows.filter(r => r.review_status === this.reviewFilter);
    if (this.hideReviewed)  rows = rows.filter(r => !r.review_status);
    if (this.filter === 'high')   return rows.filter(r => this.severity(r) === 'high');
    if (this.filter === 'medium') return rows.filter(r => this.severity(r) === 'medium');
    if (this.filter === 'low')    return rows.filter(r => this.severity(r) === 'low');
    return rows;
  }

  setReviewFilter(status: string): void {
    this.reviewFilter = this.reviewFilter === status ? null : status;
    this.hideReviewed = false;
    this.activeTab = 0;  // switch to records tab
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
    // Pre-fill notes from saved review so the user can see/edit what they wrote
    this.reviewNotes = row.review_notes || '';

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

  closeDetail(): void { this.selected = null; this.reviewNotes = ''; }

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
