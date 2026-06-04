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

  reviewing    = false;
  dismissing   = false;
  reviewNotes  = '';
  showMetrics  = false;
  hideReviewed = true;
  reviewFilter: string | null = null;
  groupByEmployee = false;
  dismissToast = false;

  // Search
  searchEmployee = '';
  searchMinistry = '';
  searchGrade    = '';

  // Dismissed list
  dismissedList: any[]    = [];
  loadingDismissed        = false;
  showDismissedTab        = false;

  private _rowFor(s: any): any {
    return this.result?.anomalies?.find((r: any) =>
      r.employee_sk === s.employee_sk &&
      r.year_num    === s.year_num    &&
      r.month_num   === s.month_num);
  }

  submitReview(status: string): void {
    if (!this.selected) return;
    this.reviewing = true;
    this.ml.submitReview(
      this.selected.employee_sk, this.selected.year_num, this.selected.month_num,
      status, this.reviewNotes
    ).subscribe({
      next: () => {
        const oldStatus: string | null = this.selected.review_status ?? null;
        const row = this._rowFor(this.selected);
        if (row) { row.review_status = status; row.review_notes = this.reviewNotes; }
        this.selected = { ...this.selected, review_status: status, review_notes: this.reviewNotes };

        // Update counts so pills and the "À examiner" badge reflect immediately
        if (this.result) {
          const stats = { ...this.result.review_stats };
          if (oldStatus && stats[oldStatus] != null) stats[oldStatus] = Math.max(0, stats[oldStatus] - 1);
          stats[status] = (stats[status] ?? 0) + 1;
          const wasUnreviewed = !oldStatus;
          this.result = {
            ...this.result,
            review_stats: stats,
            unreviewed: wasUnreviewed ? Math.max(0, (this.result.unreviewed ?? 0) - 1) : this.result.unreviewed,
          };
        }
        this.reviewing = false;
      },
      error: () => { this.reviewing = false; }
    });
  }

  clearReview(): void {
    if (!this.selected) return;
    this.ml.removeReview(this.selected.employee_sk, this.selected.year_num, this.selected.month_num)
      .subscribe({
        next: () => {
          const oldStatus: string | null = this.selected.review_status ?? null;
          const row = this._rowFor(this.selected);
          if (row) row.review_status = null;
          this.selected = { ...this.selected, review_status: null };

          // Update counts
          if (this.result && oldStatus) {
            const stats = { ...this.result.review_stats };
            if (stats[oldStatus] != null) stats[oldStatus] = Math.max(0, stats[oldStatus] - 1);
            this.result = {
              ...this.result,
              review_stats: stats,
              unreviewed: (this.result.unreviewed ?? 0) + 1,
            };
          }
        },
        error: () => {}
      });
  }

  dismissAnomaly(): void {
    if (!this.selected) return;
    this.dismissing = true;
    this.ml.dismissAnomaly(this.selected.employee_sk, this.selected.year_num, this.selected.month_num)
      .subscribe({
        next: () => {
          // Remove from list — dismissed anomalies are hidden
          if (this.result?.anomalies) {
            this.result.anomalies = this.result.anomalies.filter(
              (r: any) => !(r.employee_sk === this.selected.employee_sk &&
                            r.year_num    === this.selected.year_num    &&
                            r.month_num   === this.selected.month_num)
            );
          }
          this.dismissing = false;
          this.dismissToast = true;
          setTimeout(() => this.dismissToast = false, 5000);
          this.closeDetail();
        },
        error: () => { this.dismissing = false; }
      });
  }

  // ── Group by employee ────────────────────────────────────────────

  get employeeGroups(): { empSk: number; rows: any[] }[] {
    const rows = this.filtered;
    const map = new Map<number, any[]>();
    for (const r of rows) {
      const key = r.employee_sk;
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(r);
    }
    return Array.from(map.entries())
      .map(([empSk, rows]) => ({ empSk, rows }))
      .sort((a, b) => b.rows.length - a.rows.length);
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
    if (this.filter === 'high')   rows = rows.filter(r => this.severity(r) === 'high');
    if (this.filter === 'medium') rows = rows.filter(r => this.severity(r) === 'medium');
    if (this.filter === 'low')    rows = rows.filter(r => this.severity(r) === 'low');
    if (this.searchEmployee.trim()) {
      const q = this.searchEmployee.trim().toLowerCase();
      rows = rows.filter(r => String(r.employee_sk).includes(q));
    }
    if (this.searchMinistry.trim()) {
      const q = this.searchMinistry.trim().toLowerCase();
      rows = rows.filter(r => (r.ministry_code ?? '').toLowerCase().includes(q));
    }
    if (this.searchGrade.trim()) {
      const q = this.searchGrade.trim().toLowerCase();
      rows = rows.filter(r => (r.grade_code ?? '').toLowerCase().includes(q));
    }
    return rows;
  }

  clearSearch(): void {
    this.searchEmployee = '';
    this.searchMinistry = '';
    this.searchGrade    = '';
  }

  // ── Dismissed tab ────────────────────────────────────────────────

  openDismissedTab(): void {
    this.showDismissedTab = true;
    this.loadingDismissed = true;
    this.ml.getDismissed().subscribe({
      next: (list: any) => { this.dismissedList = list; this.loadingDismissed = false; },
      error: () => { this.loadingDismissed = false; }
    });
  }

  restoreAnomaly(r: any): void {
    this.ml.restoreAnomaly(r.employeeSk, r.yearNum, r.monthNum).subscribe({
      next: () => {
        this.dismissedList = this.dismissedList.filter(d => d !== r);
        // Add back to main anomalies list with the review status
        if (this.result?.anomalies) {
          this.result.anomalies.push({
            employee_sk:   r.employeeSk,
            year_num:      r.yearNum,
            month_num:     r.monthNum,
            review_status: r.status,
          });
        }
      },
      error: () => {}
    });
  }

  daysLeft(dismissedAt: string): number {
    const d = new Date(dismissedAt);
    const diff = 10 - Math.floor((Date.now() - d.getTime()) / 86400000);
    return Math.max(0, diff);
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
