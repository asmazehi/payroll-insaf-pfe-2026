import { Component, OnInit, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { MlService } from '../../services/ml.service';
import { AuthService } from '../../services/auth.service';
import { TranslateService } from '@ngx-translate/core';
import { EN_MINISTRIES, EN_GRADES } from './en-labels';
import { Chart, ChartData, ChartConfiguration } from 'chart.js';
import { forkJoin, of, Subscription } from 'rxjs';
import jsPDF from 'jspdf';
import html2canvas from 'html2canvas';
import zoomPlugin from 'chartjs-plugin-zoom';

Chart.register(zoomPlugin);

const MODEL_COLORS: Record<string, string> = {
  ridge:   '#94A3B8',
  rf:      '#22C55E',
  xgb:     '#F97316',
  sarima:  '#C9A84C',
  prophet: '#8B5CF6',
  tft:     '#3B82F6',
};

const METRIC_KEYS = ['MAPE', 'SMAPE', 'MAE', 'RMSE'];

@Component({
  selector: 'app-forecast',
  templateUrl: './forecast.component.html',
  styleUrls: ['./forecast.component.scss'],
})
export class ForecastComponent implements OnInit, OnDestroy {
  loading        = true;
  filterLoading  = false;
  gradesLoading  = false;
  error          = '';

  result:       any   = null;
  dimensions:   any   = null;
  filteredData: any[] = [];

  isAdminUser    = false;
  userMinistry   = '';

  selectedMinistry = '';
  selectedGrade    = '';
  availableGrades: any[] = [];
  gradeSearch      = '';

  nMonths        = 6;
  showAllHistory = false;

  showMetricInfo = false;
  activeTab      = 'forecast';

  employeeIdInput = '';
  employeeLoading = false;
  employeeError   = '';
  employeeResult: any = null;
  employeeChartData: ChartData<'line'> = { labels: [], datasets: [] };
  empPointNotes: Record<string, string> = {};

  mainChartData:     ChartData<'line'> = { labels: [], datasets: [] };
  filteredChartData: ChartData<'line'> = { labels: [], datasets: [] };


  readonly metricKeys = METRIC_KEYS;
  readonly Math       = Math;


  empChartOptions: ChartConfiguration['options'] = {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    animation: { duration: 700 },
    plugins: {
      legend: {
        display: true, position: 'top',
        labels: { font: { size: 11, family: 'Inter' }, boxWidth: 14, padding: 16, color: '#64748B' },
      },
      tooltip: {
        backgroundColor: 'rgba(13,27,42,.97)',
        titleColor: '#E8C96A', bodyColor: '#CBD5E1',
        padding: 16, cornerRadius: 10, boxPadding: 4,
        callbacks: {
          label: (c: any) => {
            const v = c.parsed.y;
            if (v == null) return '';
            const fmt = Math.round(v).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
            return `  ${c.dataset.label}: ${fmt} TND`;
          },
          afterBody: (items: any[]) => {
            const label = String(items[0]?.label || '');
            const empNote = (this as any).empPointNotes?.[label];
            if (empNote) return ['', ...(empNote as string).split('\n')];
            return [];
          },
        },
      },
      zoom: {
        pan:  { enabled: true, mode: 'x' },
        zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' },
      },
    } as any,
    scales: {
      x: { grid: { display: false }, border: { display: false }, ticks: { color: '#64748B', font: { size: 10 }, maxTicksLimit: 18 } },
      y: { grid: { color: '#F1F5F9' }, border: { display: false },
           ticks: { color: '#64748B', font: { size: 10 },
                    callback: (v: any) => Math.round(v as number).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ') + ' TND' } },
    },
  };

  private readonly PEAK_KEYS: Record<string, string> = {
    '05': 'forecast.peak_may',
    '11': 'forecast.peak_nov',
    '01': 'forecast.peak_jan',
    '07': 'forecast.peak_jul',
    '06': 'forecast.peak_jun',
  };

  chartOptions: ChartConfiguration['options'] = {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    animation: { duration: 800 },
    plugins: {
      legend: {
        display: true, position: 'top',
        labels: {
          font: { size: 12, family: 'Inter' }, boxWidth: 12, padding: 20,
          color: 'rgba(255,255,255,.55)',
          filter: (item: any) => !item.text.includes('CI '),  // hide CI bands from legend
        },
      },
      tooltip: {
        backgroundColor: 'rgba(8,8,28,.97)',
        titleColor: 'rgba(255,255,255,.9)',
        bodyColor:  'rgba(255,255,255,.65)',
        borderColor: 'rgba(255,255,255,.1)', borderWidth: 1,
        padding: 16, cornerRadius: 12, boxPadding: 5,
        callbacks: {
          title: (items: any[]) => `📅  ${items[0]?.label || ''}`,
          label: (c: any) => {
            const v = c.parsed.y;
            if (v == null) return '';
            const fmtV = (n: number) => {
              if (n >= 1e9) return (n / 1e9).toFixed(2) + ' B TND';
              if (n >= 1e6) return (n / 1e6).toFixed(2) + ' M TND';
              return Math.round(n).toLocaleString() + ' TND';
            };
            const lbl = c.dataset.label || '';
            if (lbl.includes('CI ')) return '';          // suppress CI band rows in tooltip
            const icon = lbl.includes('forecast') || lbl.includes('Forecast') || lbl.includes('Prév') ? '🔮' : '📊';
            return `  ${icon}  ${lbl.split('—')[0].trim()}: ${fmtV(v)}`;
          },
          afterBody: (items: any[]) => {
            const label: string = String(items[0]?.label || '');
            const month = label.slice(5, 7);
            const tr = (this as any).translate as TranslateService;

            const empNote = (this as any).empPointNotes?.[label];
            if (empNote) return ['', ...(empNote as string).split('\n')];

            const hist = (this as any).result?.historical || [];
            if (!hist.length) return [];
            const vals   = hist.map((r: any) => r.actual_netpay as number);
            const sorted = [...vals].sort((a: number, b: number) => a - b);
            const median = sorted[Math.floor(sorted.length / 2)];
            const val    = items.find((i: any) => i.parsed.y != null && i.datasetIndex === 0)?.parsed.y;
            const lines: string[] = [];
            const peakKey = (this as any).PEAK_KEYS[month] as string | undefined;
            const note    = peakKey ? tr.instant(peakKey) : null;
            if (note) lines.push('', `⚡ ${note}`);
            if (val != null && val > median * 1.12 && !note) lines.push('', `⬆ ${tr.instant('forecast.above_median')}`);
            if (val != null && val < median * 0.88)           lines.push('', `⬇ ${tr.instant('forecast.below_median')}`);
            return lines;
          },
        },
      },
      zoom: {
        pan:  { enabled: true,  mode: 'x' },
        zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' },
      },
    } as any,
    scales: {
      x: {
        grid: { display: false }, border: { display: false },
        ticks: { color: 'rgba(255,255,255,.3)', font: { size: 10 }, maxTicksLimit: 14, padding: 6 }
      },
      y: {
        grid: { color: 'rgba(255,255,255,.05)', drawTicks: false },
        border: { display: false },
        ticks: {
          color: 'rgba(255,255,255,.3)', font: { size: 10 }, padding: 10,
          callback: (v: any) => {
            const n = v as number;
            if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
            if (n >= 1e6) return (n / 1e6).toFixed(0) + 'M TND';
            return n.toFixed(0);
          }
        }
      }
    },
  };

  private langSub?: Subscription;

  constructor(
    private ml:        MlService,
    private translate: TranslateService,
    private cdr:       ChangeDetectorRef,
    private route:     ActivatedRoute,
    private auth:      AuthService,
  ) {}

  ngOnInit(): void {
    const user = this.auth.getCurrentUser();
    this.isAdminUser  = user?.role === 'ROLE_ADMIN';
    this.userMinistry = (!this.isAdminUser && user?.ministryCode) ? user.ministryCode : '';

    // Rebuild charts whenever language changes
    this.langSub = this.translate.onLangChange.subscribe(() => {
      if (this.result) this.buildMainChart();
      if (this.filteredData.length) this.buildFilteredChart();
      if (this.employeeResult) this.buildEmployeeChart();
      this.cdr.markForCheck();   // force dropdown options to re-render with new lang
    });

    forkJoin({
      forecast:    this.ml.getForecast(this.nMonths),
      dimensions:  this.ml.getForecastDimensions(),
      historical:  this.userMinistry ? this.ml.getForecastHistorical(this.userMinistry) : of(null),
      grades:      this.userMinistry ? this.ml.getForecastGrades(this.userMinistry)     : of(null),
    }).subscribe({
      next: ({ forecast, dimensions, historical, grades }) => {
        this.result     = forecast;
        this.dimensions = dimensions;
        this.loading    = false;
        if (this.userMinistry) {
          this.selectedMinistry = this.userMinistry;
          this.filteredData     = (historical as any)?.data || [];
          this.availableGrades  = (grades as any)?.grades  || [];
          this.buildFilteredChart();
          this.buildMainChart();
        } else {
          this.buildMainChart();
        }
      },
      error: () => {
        this.error   = this.translate.instant('forecast.ml_unavailable');
        this.loading = false;
      },
    });

    // Auto-load employee if navigated from anomaly page with ?empId=
    this.route.queryParams.subscribe(params => {
      const empId = params['empId'];
      if (empId) {
        this.employeeIdInput = String(empId);
        this.searchEmployee();
        setTimeout(() => {
          document.querySelector('.emp-search-card')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }, 600);
      }
    });
  }

  ngOnDestroy(): void {
    this.langSub?.unsubscribe();
  }

  // ── Chart builders ─────────────────────────────────────────────────────────

  toggleHistory(): void {
    this.showAllHistory = !this.showAllHistory;
    this.buildMainChart();
  }

  buildMainChart(): void {
    // For non-admin users locked to a ministry, use ministry-filtered historical data
    const useFiltered = !!this.userMinistry && this.filteredData.length > 0;
    const all  = useFiltered ? this.filteredData : (this.result?.historical || []);
    const hist = this.showAllHistory ? all : all.slice(-48);
    const globalFore  = this.result?.forecast || [];

    // Scale forecast by ministry share so the dashed line stays contextually relevant
    let fore = globalFore;
    if (useFiltered && globalFore.length && all.length) {
      const filteredAvg = all.reduce((s: number, r: any) => s + r.actual_netpay, 0) / all.length;
      const globalHist  = this.result?.historical || [];
      const globalAvg   = globalHist.length
        ? globalHist.reduce((s: number, r: any) => s + r.actual_netpay, 0) / globalHist.length
        : 1;
      const ratio = globalAvg > 0 ? filteredAvg / globalAvg : 1;
      fore = globalFore.map((r: any) => ({
        ...r,
        predicted_netpay: r.predicted_netpay * ratio,
        upper: r.upper * ratio,
        lower: r.lower * ratio,
      }));
    }

    const histLabels = hist.map((r: any) => r.date);
    const foreLabels = fore.map((r: any) => r.date);
    const pad  = (arr: any[], n: number) => [...Array(n).fill(null), ...arr];
    const padH = (arr: any[], n: number) => [...arr, ...Array(n).fill(null)];

    const t = (k: string) => this.translate.instant(k);

    const HIST_COLOR = '#6366f1';
    const FORE_COLOR = '#f59e0b';

    this.mainChartData = {
      labels: [...histLabels, ...foreLabels],
      datasets: [
        // ── Historical payroll ──────────────────────────────────────────────
        {
          label: t('forecast.chart_hist'),
          data: padH(hist.map((r: any) => r.actual_netpay), foreLabels.length),
          borderColor: HIST_COLOR,
          backgroundColor: (ctx: any) => {
            const c = ctx.chart.ctx;
            const g = c.createLinearGradient(0, 0, 0, ctx.chart.height);
            g.addColorStop(0, 'rgba(99,102,241,.28)');
            g.addColorStop(1, 'rgba(99,102,241,.01)');
            return g;
          },
          borderWidth: 2.5, pointRadius: 0, pointHoverRadius: 5,
          pointHoverBackgroundColor: HIST_COLOR, pointHoverBorderColor: '#fff', pointHoverBorderWidth: 2,
          tension: 0.38, fill: true, spanGaps: true,
        },
        // ── CI upper — no visible line, just anchor for fill ────────────────
        {
          label: 'CI upper',
          data: pad(fore.map((r: any) => r.upper), histLabels.length),
          borderColor: 'transparent',
          backgroundColor: 'transparent',
          borderWidth: 0, pointRadius: 0, tension: 0.3,
          spanGaps: false, fill: false,
        },
        // ── CI lower — fills between itself and CI upper ─────────────────────
        {
          label: 'CI lower',
          data: pad(fore.map((r: any) => r.lower), histLabels.length),
          borderColor: 'transparent',
          backgroundColor: 'rgba(245,158,11,.13)',
          borderWidth: 0, pointRadius: 0, tension: 0.3,
          fill: '-1', spanGaps: false,
        },
        // ── Forecast ────────────────────────────────────────────────────────
        {
          label: `${t('forecast.chart_forecast')} — ${this.winnerLabel} (MAPE ${this.result?.mape?.toFixed(1)}%)`,
          data: pad(fore.map((r: any) => r.predicted_netpay), histLabels.length),
          borderColor: FORE_COLOR,
          backgroundColor: 'transparent',
          borderWidth: 2.5, borderDash: [7, 4],
          pointBackgroundColor: FORE_COLOR, pointRadius: 4, pointHoverRadius: 6,
          pointBorderColor: '#0e0e1a', pointBorderWidth: 1.5,
          tension: 0.3, fill: false, spanGaps: false,
        },
      ] as any,
    };
  }

  buildFilteredChart(): void {
    const data = this.filteredData.slice(-48);
    if (!data.length) { this.filteredChartData = { labels: [], datasets: [] }; return; }

    const filterLabel = this.selectedMinistry
      ? this.ministryName(this.selectedMinistry)
      : this.gradeLabel(this.selectedGrade);

    const globalFore  = this.result?.forecast || [];
    const globalAvg   = (this.result?.historical || []).reduce((s: number, r: any) => s + r.actual_netpay, 0)
                        / Math.max(1, (this.result?.historical || []).length);
    const ministryAvg = data.reduce((s: number, r: any) => s + r.actual_netpay, 0) / data.length;
    const ratio       = globalAvg > 0 ? ministryAvg / globalAvg : 1;
    const fore        = globalFore.map((r: any) => ({
      ...r,
      predicted_netpay: r.predicted_netpay * ratio,
      upper: r.upper * ratio,
      lower: r.lower * ratio,
    }));

    const histLabels = data.map((r: any) => r.date);
    const foreLabels = fore.map((r: any) => r.date);
    const pad  = (arr: any[], n: number) => [...Array(n).fill(null), ...arr];
    const padH = (arr: any[], n: number) => [...arr, ...Array(n).fill(null)];
    const FORE_COLOR = '#f59e0b';

    this.filteredChartData = {
      labels: [...histLabels, ...foreLabels],
      datasets: [
        {
          label: `${filterLabel} — ${this.translate.instant('forecast.chart_filtered')}`,
          data: padH(data.map((r: any) => r.actual_netpay), foreLabels.length),
          borderColor: '#8B5CF6',
          backgroundColor: (ctx: any) => {
            const c = ctx.chart.ctx;
            const g = c.createLinearGradient(0, 0, 0, ctx.chart.height);
            g.addColorStop(0, 'rgba(139,92,246,.3)');
            g.addColorStop(1, 'rgba(139,92,246,.01)');
            return g;
          },
          borderWidth: 2.5, pointRadius: 0, pointHoverRadius: 5,
          pointHoverBackgroundColor: '#8B5CF6', tension: 0.38, fill: true, spanGaps: true,
        },
        {
          label: 'CI upper',
          data: pad(fore.map((r: any) => r.upper), histLabels.length),
          borderColor: 'transparent', backgroundColor: 'transparent',
          borderWidth: 0, pointRadius: 0, tension: 0.3, spanGaps: false, fill: false,
        },
        {
          label: 'CI lower',
          data: pad(fore.map((r: any) => r.lower), histLabels.length),
          borderColor: 'transparent', backgroundColor: 'rgba(245,158,11,.13)',
          borderWidth: 0, pointRadius: 0, tension: 0.3, fill: '-1', spanGaps: false,
        },
        {
          label: `${this.translate.instant('forecast.chart_forecast')} — ${this.winnerLabel} (MAPE ${this.result?.mape?.toFixed(1)}%)`,
          data: pad(fore.map((r: any) => r.predicted_netpay), histLabels.length),
          borderColor: FORE_COLOR, backgroundColor: 'transparent',
          borderWidth: 2.5, borderDash: [7, 4],
          pointBackgroundColor: FORE_COLOR, pointRadius: 4, pointHoverRadius: 6,
          pointBorderColor: '#0e0e1a', pointBorderWidth: 1.5,
          tension: 0.3, fill: false, spanGaps: false,
        },
      ] as any,
    };
  }

  buildEmployeeChart(): void {
    const res  = this.employeeResult;
    if (!res) return;
    const hist = res.historical || [];
    const fore = res.forecast   || [];

    // ── Build rich per-point explanations ─────────────────────────────────
    const notes: Record<string, string> = {};

    // Pre-compute median (ignores outliers better than mean)
    const sortedPay = [...hist].map((r: any) => r.netpay as number).sort((a, b) => a - b);
    const medianPay = sortedPay[Math.floor(sortedPay.length / 2)] || 0;

    // Average pay for each calendar month across all years (detects recurring patterns)
    const byCalMonth: Record<string, number[]> = {};
    hist.forEach((r: any) => {
      const m = r.date.slice(5, 7);
      (byCalMonth[m] = byCalMonth[m] || []).push(r.netpay);
    });
    const calMonthAvg = (m: string) => {
      const arr = byCalMonth[m] || [];
      return arr.length ? arr.reduce((s: number, v: number) => s + v, 0) / arr.length : medianPay;
    };

    hist.forEach((r: any, i: number) => {
      const prev      = i > 0 ? hist[i - 1].netpay : null;
      const next      = i < hist.length - 1 ? hist[i + 1].netpay : null;
      const monthStr  = r.date.slice(5, 7);
      const momPct    = prev && prev > 0 ? ((r.netpay - prev) / prev * 100) : null;

      if (momPct === null || Math.abs(momPct) < 5) return;

      const absChg = `${momPct >= 0 ? '+' : ''}${momPct.toFixed(1)}%`;
      const fmtTnd = (v: number) => Math.round(v).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ') + ' TND';
      const lines: string[] = [];

      // ── Classify the change ──────────────────────────────────────────────

      if (momPct > 0) {
        // Is this calendar month CONSISTENTLY higher than the month before?
        const prevMonthStr = monthStr === '01' ? '12' : String(parseInt(monthStr) - 1).padStart(2, '0');
        const avgThisMonth = calMonthAvg(monthStr);
        const avgPrevMonth = calMonthAvg(prevMonthStr);
        const isRecurring  = (byCalMonth[monthStr]?.length || 0) >= 2 &&
                             avgThisMonth > avgPrevMonth * 1.07;

        // Does pay return to previous level next month (one-time payment)?
        const isOneTime    = next !== null && prev !== null &&
                             next < r.netpay * 0.85 &&
                             Math.abs(next - prev) / (prev || 1) < 0.08;

        // Does the raise persist for the next 3+ months (permanent step-up)?
        const next3        = hist.slice(i + 1, i + 4).map((h: any) => h.netpay);
        const isPersistent = next3.length >= 2 && next3.every((v: number) => v >= r.netpay * 0.93);

        const ti = (k: string, p?: any) => this.translate.instant(k, p);
        if (isRecurring && (monthStr === '05' || monthStr === '06')) {
          lines.push(ti('forecast.note.recurring_may', { date: r.date.slice(0, 7) }));
          lines.push(ti('forecast.note.recurring_may_why'));
          lines.push(ti('forecast.note.recurring_may_avg', { amount: fmtTnd(avgThisMonth) }));
        } else if (isRecurring && monthStr === '11') {
          lines.push(ti('forecast.note.recurring_nov'));
          lines.push(ti('forecast.note.recurring_nov_why'));
          lines.push(ti('forecast.note.recurring_nov_avg', { amount: fmtTnd(avgThisMonth) }));
        } else if (isRecurring) {
          lines.push(ti('forecast.note.recurring_generic'));
          lines.push(ti('forecast.note.recurring_generic_why', { pct: absChg }));
        } else if (isOneTime) {
          lines.push(ti('forecast.note.onetime'));
          lines.push(ti('forecast.note.onetime_why'));
          if (prev) lines.push(ti('forecast.note.onetime_detail', { before: fmtTnd(prev), current: fmtTnd(r.netpay), after: fmtTnd(next!) }));
        } else if (isPersistent) {
          lines.push(ti('forecast.note.persistent'));
          if (momPct >= 15) lines.push(ti('forecast.note.persistent_big_why'));
          else              lines.push(ti('forecast.note.persistent_small_why'));
          if (prev) lines.push(ti('forecast.note.persistent_detail', { before: fmtTnd(prev), after: fmtTnd(r.netpay) }));
        } else {
          lines.push(ti('forecast.note.generic_rise', { pct: absChg }));
          lines.push(ti('forecast.note.generic_rise_why'));
        }
      } else {
        // DROP
        const prevMom    = i > 1 ? hist[i - 1].netpay : null;
        const prevPrevMom = i > 1 ? hist[i - 2]?.netpay : null;
        const wasAfterSpike = prevMom !== null && prevPrevMom !== null &&
                              prevMom > prevPrevMom * 1.10;

        // Does salary stabilise at this lower level?
        const next3     = hist.slice(i + 1, i + 4).map((h: any) => h.netpay);
        const staysLow  = next3.length >= 2 && next3.every((v: number) => v <= r.netpay * 1.08);

        const ti2 = (k: string, p?: any) => this.translate.instant(k, p);
        if (wasAfterSpike) {
          lines.push(ti2('forecast.note.drop_after_spike'));
          lines.push(ti2('forecast.note.drop_after_spike_why'));
          if (prev) lines.push(ti2('forecast.note.drop_after_spike_detail', { bonus: fmtTnd(prev), normal: fmtTnd(r.netpay) }));
        } else if (staysLow) {
          lines.push(ti2('forecast.note.drop_persistent'));
          lines.push(ti2('forecast.note.drop_persistent_why'));
          lines.push(ti2('forecast.note.drop_persistent_why2'));
        } else {
          lines.push(ti2('forecast.note.drop_generic', { pct: absChg }));
          lines.push(ti2('forecast.note.drop_generic_why'));
        }
      }

      notes[r.date] = lines.join('\n');
    });
    this.empPointNotes = notes;

    const histLabels = hist.map((r: any) => r.date);
    const foreLabels = fore.map((r: any) => r.date);
    const pad  = (arr: any[], n: number) => [...Array(n).fill(null), ...arr];
    const padH = (arr: any[], n: number) => [...arr, ...Array(n).fill(null)];
    const t    = (k: string) => this.translate.instant(k);

    this.employeeChartData = {
      labels: [...histLabels, ...foreLabels],
      datasets: [
        {
          label: t('forecast.chart_emp_hist'),
          data: padH(hist.map((r: any) => r.netpay), foreLabels.length),
          borderColor: '#1E3048', backgroundColor: 'rgba(30,48,72,.07)',
          borderWidth: 2, pointRadius: 2, tension: 0.3, fill: true, spanGaps: true,
        },
        {
          label: t('forecast.chart_emp_proj'),
          data: pad(fore.map((r: any) => r.netpay), histLabels.length),
          borderColor: '#C9A84C', backgroundColor: 'rgba(201,168,76,.10)',
          borderWidth: 2.5, borderDash: [7, 4] as any,
          pointBackgroundColor: '#C9A84C', pointRadius: 5, tension: 0.2, fill: true, spanGaps: true,
        },
        {
          label: t('forecast.chart_ci_upper'),
          data: pad(fore.map((r: any) => r.upper), histLabels.length),
          borderColor: 'rgba(201,168,76,.25)', backgroundColor: 'transparent',
          borderWidth: 1, borderDash: [3, 3] as any, pointRadius: 0, tension: 0.2, spanGaps: true,
        },
        {
          label: t('forecast.chart_ci_lower'),
          data: pad(fore.map((r: any) => r.lower), histLabels.length),
          borderColor: 'rgba(201,168,76,.25)', backgroundColor: 'rgba(201,168,76,.06)',
          borderWidth: 1, borderDash: [3, 3] as any, pointRadius: 0, tension: 0.2, fill: '-1', spanGaps: true,
        },
      ] as any,
    };
  }

  // ── Filter actions ─────────────────────────────────────────────────────────

  onMinistryChange(): void {
    this.selectedGrade   = '';
    this.availableGrades = [];
    this.gradeSearch     = '';
    if (!this.selectedMinistry) {
      this.filteredData      = [];
      this.filteredChartData = { labels: [], datasets: [] };
      return;
    }
    this.gradesLoading = true;
    this.ml.getForecastGrades(this.selectedMinistry).subscribe({
      next: (res: any) => {
        this.availableGrades = res.grades || [];
        this.gradesLoading   = false;
        this.applyFilter();
      },
      error: () => { this.gradesLoading = false; this.applyFilter(); },
    });
  }

  applyFilter(): void {
    if (!this.selectedMinistry && !this.selectedGrade) {
      this.filteredData      = [];
      this.filteredChartData = { labels: [], datasets: [] };
      return;
    }
    this.filterLoading = true;
    this.ml.getForecastHistorical(
      this.selectedMinistry || undefined,
      this.selectedGrade    || undefined,
    ).subscribe({
      next: (res: any) => {
        this.filteredData  = res.data || [];
        this.filterLoading = false;
        this.buildFilteredChart();
        // Refresh main chart so it uses ministry data for non-admin users
        if (this.userMinistry && this.result) this.buildMainChart();
      },
      error: () => { this.filterLoading = false; },
    });
  }

  clearFilters(): void {
    this.selectedMinistry  = '';
    this.selectedGrade     = '';
    this.availableGrades   = [];
    this.filteredData      = [];
    this.filteredChartData = { labels: [], datasets: [] };
  }

  // ── Employee forecast ──────────────────────────────────────────────────────

  searchEmployee(): void {
    const id = this.employeeIdInput.trim();
    if (!id) return;
    this.employeeLoading = true;
    this.employeeError   = '';
    this.employeeResult  = null;
    this.employeeChartData = { labels: [], datasets: [] };

    this.ml.getEmployeeForecast(id).subscribe({
      next: (res: any) => {
        this.employeeResult  = res;
        this.employeeLoading = false;
        this.buildEmployeeChart();
      },
      error: (err: any) => {
        this.employeeError   = err?.error?.detail || this.translate.instant('forecast.emp_no_data');
        this.employeeLoading = false;
      },
    });
  }

  clearEmployee(): void {
    this.employeeIdInput   = '';
    this.employeeResult    = null;
    this.employeeError     = '';
    this.employeeChartData = { labels: [], datasets: [] };
  }

  // ── Horizon selector ──────────────────────────────────────────────────────

  onHorizonChange(n: number): void {
    if (n === this.nMonths) return;
    this.nMonths = n;
    this.loading = true;
    this.ml.getForecast(n).subscribe({
      next: (forecast: any) => {
        this.result  = forecast;
        this.loading = false;
        this.buildMainChart();
        if (this.filteredData.length) this.buildFilteredChart();
      },
      error: () => { this.loading = false; },
    });
  }

  // ── Grade search ───────────────────────────────────────────────────────────

  get filteredAvailableGrades(): any[] {
    const q = this.gradeSearch.trim().toLowerCase();
    if (!q) return this.availableGrades;
    return this.availableGrades.filter(g =>
      g.code.toLowerCase().includes(q) ||
      this.gradeLabel(g.code).toLowerCase().includes(q)
    );
  }

  // ── Data freshness & split info ────────────────────────────────────────────

  get lastDataDate(): string { return this.result?.last_data_date || ''; }

  get splitLabel(): string {
    const tr = this.result?.train_months || 0;
    const te = this.result?.test_months  || 0;
    const tot = tr + te;
    if (!tot) return '';
    return `${Math.round(tr / tot * 100)}/${Math.round(te / tot * 100)}`;
  }

  // ── Export CSV ─────────────────────────────────────────────────────────────

  exportForecastCsv(): void {
    const hist = (this.result?.historical || []).map((r: any) =>
      `${r.date},${r.actual_netpay},${r.employee_count},${r.avg_netpay},historical`);
    const fore = (this.result?.forecast || []).map((r: any) =>
      `${r.date},${r.predicted_netpay},,,forecast,${r.lower},${r.upper}`);
    const header = 'date,netpay,employee_count,avg_netpay,type,ci_lower,ci_upper';
    const csv    = [header, ...hist, ...fore].join('\n');
    this._downloadCsv(csv, `payroll_forecast_${this.result?.model}_${this.nMonths}m.csv`);
  }

  exportEmployeeCsv(): void {
    if (!this.employeeResult) return;
    const hist = (this.employeeResult.historical || []).map((r: any) =>
      `${r.date},${r.netpay},,historical`);
    const fore = (this.employeeResult.forecast || []).map((r: any) =>
      `${r.date},${r.netpay},${r.lower ?? ''},${r.upper ?? ''},forecast`);
    const header = `date,netpay,ci_lower,ci_upper,type`;
    const csv    = [header, ...hist, ...fore].join('\n');
    this._downloadCsv(csv, `employee_${this.employeeResult.employee_id}_forecast.csv`);
  }

  private _downloadCsv(content: string, filename: string): void {
    const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
  }

  // ── PDF exports ────────────────────────────────────────────────────────────

  async exportEmployeePdf(): Promise<void> {
    if (!this.employeeResult) return;
    const res = this.employeeResult;
    const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' });
    const W   = doc.internal.pageSize.getWidth();

    // Palette — all on white paper, dark text
    const GOLD     = '#C9A84C';
    const DARK_TXT = '#1E293B';
    const MID_TXT  = '#475569';
    const LIGHT_BG = '#F8FAFC';
    const BORDER   = '#E2E8F0';
    const GREEN_TXT= '#15803D';
    const RED_TXT  = '#991B1B';
    const BLUE_TXT = '#1D4ED8';

    // Plain ASCII number formatter — avoids Unicode non-breaking spaces that break jsPDF fonts
    const n = (v: number) => v ? Math.round(v).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ') + ' TND' : '—';

    const setRGB = (hex: string) => {
      const r = parseInt(hex.slice(1,3),16);
      const g = parseInt(hex.slice(3,5),16);
      const b = parseInt(hex.slice(5,7),16);
      return [r, g, b] as [number, number, number];
    };

    const drawHLine = (yy: number, color = BORDER) => {
      const [r,g,b] = setRGB(color);
      doc.setDrawColor(r,g,b); doc.setLineWidth(0.3);
      doc.line(14, yy, W - 14, yy);
    };

    // ── Header strip ─────────────────────────────────────────────────────
    const [gr, gg, gb] = setRGB(GOLD);
    doc.setFillColor(gr, gg, gb);
    doc.rect(0, 0, W, 2, 'F');                      // thin gold top bar
    doc.setFillColor(248, 250, 252);                 // light grey bg
    doc.rect(0, 2, W, 38, 'F');
    doc.setFillColor(gr, gg, gb);
    doc.rect(0, 38, W, 1.5, 'F');                   // gold bottom line

    // Logo area — left colour block
    doc.setFillColor(gr, gg, gb);
    doc.rect(0, 2, 5, 38, 'F');

    // Title
    const [dr, dg, db] = setRGB(DARK_TXT);
    doc.setTextColor(dr, dg, db);
    doc.setFontSize(18); doc.setFont('helvetica', 'bold');
    doc.text('Bulletin de Prévision Salariale', 12, 17);

    doc.setFontSize(11); doc.setFont('helvetica', 'normal');
    const [mr, mg, mb] = setRGB(MID_TXT);
    doc.setTextColor(mr, mg, mb);
    const fullName = `${res.first_name || ''} ${res.last_name || ''}`.trim() || res.employee_id;
    doc.text(fullName, 12, 25);
    doc.setFontSize(9);
    const gradeInfo = [res.grade_label || res.grade_code, res.ministry].filter(Boolean).join('  ·  ');
    doc.text(gradeInfo, 12, 31);

    const dateStr = new Date().toLocaleDateString('fr-TN', { year:'numeric', month:'long', day:'numeric' });
    doc.setFontSize(8);
    doc.text(`Émis le ${dateStr}`, W - 14, 31, { align: 'right' });

    let y = 50;

    // ── Pay history chart ──────────────────────────────────────────────────
    const empCanvas = document.querySelector('.emp-search-card canvas') as HTMLCanvasElement | null;
    if (empCanvas) {
      const imgData = empCanvas.toDataURL('image/png');
      const iW = W - 28;
      const iH = 65;
      // chart border
      const [br, bg, bb] = setRGB(BORDER);
      doc.setDrawColor(br, bg, bb); doc.setLineWidth(0.4);
      doc.rect(14, y, iW, iH + 2);
      doc.addImage(imgData, 'PNG', 14, y + 1, iW, iH);
      doc.setFontSize(8); doc.setFont('helvetica', 'italic');
      doc.setTextColor(mr, mg, mb);
      doc.text('Évolution du salaire net — historique et prévisions sur 3 mois', 14, y + iH + 8);
      y += iH + 14;
    }

    // ── 3-month salary forecast ────────────────────────────────────────────
    drawHLine(y);
    y += 7;
    doc.setTextColor(gr, gg, gb);
    doc.setFontSize(13); doc.setFont('helvetica', 'bold');
    doc.text('Prévisions salariales — 3 prochains mois', 14, y);
    y += 8;
    doc.setFontSize(9); doc.setFont('helvetica', 'normal');
    doc.setTextColor(mr, mg, mb);
    doc.text('Estimation du salaire net pour les trois prochains mois, basée sur votre historique de paie.', 14, y);
    y += 8;

    // Table header
    const [lbr, lbg, lbb] = setRGB(LIGHT_BG);
    doc.setFillColor(lbr, lbg, lbb);
    doc.rect(14, y, W - 28, 9, 'F');
    drawHLine(y);
    drawHLine(y + 9);

    doc.setTextColor(mr, mg, mb);
    doc.setFontSize(8.5); doc.setFont('helvetica', 'bold');
    const fCols  = ['Mois', 'Salaire estimé', 'Fourchette basse', 'Fourchette haute'];
    const fColX  = [14, 60, 110, 155];
    fCols.forEach((c, i) => doc.text(c, fColX[i] + 2, y + 6));
    y += 9;

    const fore3 = (res.forecast || []).slice(0, 3);
    fore3.forEach((r: any, i: number) => {
      if (i % 2 === 0) {
        doc.setFillColor(lbr, lbg, lbb);
        doc.rect(14, y, W - 28, 10, 'F');
      }
      drawHLine(y + 10, BORDER);
      doc.setTextColor(dr, dg, db); doc.setFontSize(9); doc.setFont('helvetica', 'bold');
      doc.text(r.date, fColX[0] + 2, y + 7);
      doc.setFont('helvetica', 'bold');
      doc.setTextColor(...setRGB(BLUE_TXT));
      doc.text(n(r.netpay), fColX[1] + 2, y + 7);
      doc.setFont('helvetica', 'normal');
      doc.setTextColor(mr, mg, mb);
      doc.text(r.lower ? n(r.lower) : '—', fColX[2] + 2, y + 7);
      doc.text(r.upper ? n(r.upper) : '—', fColX[3] + 2, y + 7);
      y += 10;
    });

    // Note under table
    y += 5;
    doc.setFontSize(8); doc.setFont('helvetica', 'italic');
    doc.setTextColor(mr, mg, mb);
    doc.text('La fourchette indique une marge raisonnable autour de l\'estimation (ni minimum ni maximum garanti).', 14, y);
    y += 12;

    // ── Pay history (last 12 months) ───────────────────────────────────────
    if (y > 200) { doc.addPage(); y = 20; }

    drawHLine(y);
    y += 7;
    doc.setTextColor(gr, gg, gb);
    doc.setFontSize(13); doc.setFont('helvetica', 'bold');
    doc.text('Historique de paie — 12 derniers mois', 14, y);
    y += 8;

    // Table header
    doc.setFillColor(lbr, lbg, lbb);
    doc.rect(14, y, W - 28, 9, 'F');
    drawHLine(y); drawHLine(y + 9);
    doc.setTextColor(mr, mg, mb);
    doc.setFontSize(8.5); doc.setFont('helvetica', 'bold');
    const hCols = ['Mois', 'Salaire net (TND)', 'Variation mensuelle'];
    const hColX = [14, 75, 140];
    hCols.forEach((c, i) => doc.text(c, hColX[i] + 2, y + 6));
    y += 9;

    const hist12 = (res.historical || []).slice(-12);
    hist12.forEach((r: any, i: number) => {
      if (y > 270) { doc.addPage(); y = 20; }
      if (i % 2 === 0) {
        doc.setFillColor(lbr, lbg, lbb);
        doc.rect(14, y, W - 28, 9, 'F');
      }
      drawHLine(y + 9, BORDER);
      const prev = i > 0 ? hist12[i - 1].netpay : null;
      const momVal = prev ? ((r.netpay - prev) / prev * 100) : null;
      const momStr = momVal != null ? `${momVal >= 0 ? '+' : ''}${momVal.toFixed(1)}%` : '—';
      const momColor = momVal == null ? MID_TXT : momVal >= 0 ? GREEN_TXT : RED_TXT;

      doc.setTextColor(dr, dg, db); doc.setFontSize(9); doc.setFont('helvetica', 'bold');
      doc.text(r.date, hColX[0] + 2, y + 6.5);
      doc.setFont('helvetica', 'normal');
      doc.text(n(r.netpay), hColX[1] + 2, y + 6.5);
      doc.setTextColor(...setRGB(momColor));
      doc.setFont('helvetica', 'bold');
      doc.text(momStr, hColX[2] + 2, y + 6.5);
      y += 9;
    });

    // ── Footer ─────────────────────────────────────────────────────────────
    const totalPages = (doc as any).internal.getNumberOfPages();
    for (let p = 1; p <= totalPages; p++) {
      doc.setPage(p);
      doc.setFillColor(lbr, lbg, lbb);
      doc.rect(0, 285, W, 12, 'F');
      doc.setFillColor(gr, gg, gb); doc.rect(0, 285, W, 1, 'F');
      doc.setTextColor(mr, mg, mb); doc.setFontSize(7.5); doc.setFont('helvetica', 'normal');
      doc.text('Document confidentiel — Usage interne uniquement', 14, 291);
      doc.text(`Page ${p} / ${totalPages}`, W - 14, 291, { align: 'right' });
    }

    doc.save(`prevision_${res.employee_id}_${new Date().toISOString().slice(0,10)}.pdf`);
  }


  // ── Helpers ────────────────────────────────────────────────────────────────

  get winnerKey():   string { return this.result?.model || ''; }
  get winnerLabel(): string {
    const k = this.winnerKey;
    if (!k) return '';
    return this.translate.instant(`forecast.models.${k}.label`);
  }

  get allMetrics(): any[] {
    const cm = this.result?.model_comparison || {};
    return Object.entries(cm)
      .map(([model, m]: any) => ({ model, ...m }))
      .sort((a, b) => {
        if (a.model === this.winnerKey) return -1;
        if (b.model === this.winnerKey) return  1;
        if (a.model === 'ridge')        return  1;
        if (b.model === 'ridge')        return -1;
        return (a.mape ?? 999) - (b.mape ?? 999);
      });
  }

  get filteredAvg(): number {
    if (!this.filteredData.length) return 0;
    return this.filteredData.reduce((s: number, r: any) => s + r.actual_netpay, 0) / this.filteredData.length;
  }
  get filteredShare(): number {
    if (!this.result?.avg_historical || !this.filteredAvg) return 0;
    return (this.filteredAvg / this.result.avg_historical) * 100;
  }
  get filteredForecastEstimate(): number {
    return (this.filteredShare / 100) * (this.result?.avg_forecast || 0);
  }

  /** KPI: avg historical — ministry-specific for locked users, global otherwise */
  get displayAvgHistorical(): number {
    return (this.userMinistry && this.filteredData.length)
      ? this.filteredAvg
      : (this.result?.avg_historical || 0);
  }
  /** KPI: avg forecast — ministry-scaled for locked users, global otherwise */
  get displayAvgForecast(): number {
    return (this.userMinistry && this.filteredData.length)
      ? this.filteredForecastEstimate
      : (this.result?.avg_forecast || 0);
  }

  get empYoyPct(): string {
    const r = this.employeeResult?.yoy_rate;
    if (!r || isNaN(r)) return '0.0';
    return ((r - 1) * 100).toFixed(1);
  }

  colorOf(model: string): string { return MODEL_COLORS[model] || '#94A3B8'; }
  isWinner(m: string): boolean   { return m === this.winnerKey; }
  isRidge(m: string):  boolean   { return m === 'ridge'; }

  get currentLang(): string { return this.translate.currentLang || 'fr'; }

  // Ministry/grade names exist only in French and Arabic in the DB.
  // English falls back to French (official Tunisian administrative language).
  localLabel(fr: string, ar: string): string {
    if (this.currentLang === 'ar') return ar || fr || '';
    return fr || ar || '';
  }

  ministryName(code: string): string {
    if (this.currentLang === 'en' && EN_MINISTRIES[code]) return EN_MINISTRIES[code];
    const found = (this.dimensions?.ministries || []).find((m: any) => m.code === code);
    if (!found) return code;
    return this.localLabel(found.name_fr, found.name_ar);
  }
  gradeLabel(code: string): string {
    if (this.currentLang === 'en' && EN_GRADES[code]) return EN_GRADES[code];
    const found = (this.dimensions?.grades || []).find((g: any) => g.code === code);
    if (!found) return code;
    return this.localLabel(found.label_fr, found.label_ar);
  }

  fmt(n: number):    string { if (!n) return '—'; return (n / 1e6).toFixed(1) + ' M TND'; }
  fmtPct(n: number): string { return n != null ? n.toFixed(2) + '%' : '—'; }
  fmtNum(n: number): string { return n ? n.toLocaleString('fr-TN', { maximumFractionDigits: 0 }) : '—'; }

}
