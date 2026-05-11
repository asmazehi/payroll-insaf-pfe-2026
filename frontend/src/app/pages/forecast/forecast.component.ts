import { Component, OnInit } from '@angular/core';
import { MlService } from '../../services/ml.service';
import { ChartData, ChartConfiguration } from 'chart.js';
import { forkJoin } from 'rxjs';

interface ModelMeta {
  label:       string;
  description: string;
  color:       string;
  type:        string;
}

const MODEL_META: Record<string, ModelMeta> = {
  ridge: {
    label: 'Ridge Regression', type: 'Linear',
    color: '#94A3B8',
    description: 'Linear model with L2 regularization. Assumes a straight-line relationship between lag features and payroll. Extremely fast but lacks the ability to capture seasonality. A perfect R² here signals overfitting — the model memorised the training set rather than learning true patterns, so it is excluded from winner selection.',
  },
  rf: {
    label: 'Random Forest', type: 'Ensemble',
    color: '#22C55E',
    description: 'Ensemble of 100 decision trees, each trained on a random subset of the 12-month lag features, rolling averages, and seasonal signals. Robust to outliers and captures non-linear relationships. However, tree-based models cannot extrapolate beyond their training range, which limits accuracy for long-term upward trends.',
  },
  xgb: {
    label: 'XGBoost', type: 'Boosting',
    color: '#F97316',
    description: 'Gradient Boosting — sequentially builds weak learners where each corrects the errors of the previous. State-of-the-art on tabular data competitions. More prone to overfitting than Random Forest when the time series is short (< 200 points).',
  },
  sarima: {
    label: 'SARIMA', type: 'Statistical',
    color: '#C9A84C',
    description: 'Seasonal ARIMA — the gold standard for monthly time series. Explicitly models three components: trend (AR terms), integration (differencing for stationarity), and seasonality (SAR/SMA terms with period = 12 months). Highly interpretable and consistently reliable for payroll data. Selected as winner when MAPE is lowest.',
  },
  prophet: {
    label: 'Prophet (Meta)', type: 'Statistical',
    color: '#8B5CF6',
    description: 'Open-source model by Meta (Facebook). Decomposes the series into trend + yearly seasonality + weekly seasonality + holidays. Robust to missing data and outliers. Well-suited for business forecasting but can be outperformed by SARIMA when data is clean and seasonal patterns are regular.',
  },
  tft: {
    label: 'TFT (Deep Learning)', type: 'Deep Learning',
    color: '#3B82F6',
    description: 'Temporal Fusion Transformer — state-of-the-art deep learning model for multi-horizon time series. Uses multi-head self-attention to weight past time steps, gating mechanisms to select relevant features, and static covariate encoders. Requires large datasets (thousands of series) to outperform classical methods; on 136 monthly data points it tends to underfit.',
  },
};

const METRIC_INFO: Record<string, string> = {
  MAPE: 'Mean Absolute Percentage Error — the average % difference between predicted and actual values. E.g., 5% means the model is off by 5% on average. Lower is better. This is the primary ranking metric.',
  MAE:  'Mean Absolute Error — average absolute error in TND. E.g., 26M TND means predictions deviate by ~26 million dinars per month on average. Same unit as payroll, easy to interpret.',
  RMSE: 'Root Mean Square Error — like MAE but squares errors first, so large mistakes are penalised more heavily. Useful for detecting models that occasionally produce very wrong predictions.',
  'R2':  'R-squared (R²) — coefficient of determination. Range: -inf to 1. A value of 1 = perfect fit. A value of 0 = same as predicting the historical mean every month. Negative values mean the model performs worse than the naive mean — common for time series where variance is high and the test period is short.',
};

@Component({
  selector: 'app-forecast',
  templateUrl: './forecast.component.html',
  styleUrls: ['./forecast.component.scss'],
})
export class ForecastComponent implements OnInit {
  loading        = true;
  filterLoading  = false;
  gradesLoading  = false;
  error          = '';

  result:       any    = null;
  dimensions:   any    = null;
  filteredData: any[]  = [];

  // Filters
  selectedMinistry  = '';
  selectedGrade     = '';
  availableGrades:  any[] = [];   // scoped to selected ministry

  // UI state
  showMetricInfo = false;
  activeTab      = 'forecast';

  // Charts
  mainChartData:    ChartData<'line'> = { labels: [], datasets: [] };
  filteredChartData: ChartData<'line'> = { labels: [], datasets: [] };

  chartOptions: ChartConfiguration['options'] = {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    animation: { duration: 700 },
    plugins: {
      legend: {
        display: true, position: 'top',
        labels: { font: { size: 11, family: 'Inter' }, boxWidth: 14, padding: 16, color: '#64748B' },
      },
      tooltip: {
        backgroundColor: 'rgba(13,27,42,.95)',
        titleColor: '#E8C96A', bodyColor: '#CBD5E1',
        padding: 14, cornerRadius: 10,
        callbacks: {
          label: (c) => `  ${c.dataset.label}: ${Number(c.parsed.y).toLocaleString('fr-TN', { maximumFractionDigits: 0 })} TND`,
        },
      },
    },
    scales: {
      x: { grid: { display: false }, border: { display: false }, ticks: { color: '#64748B', font: { size: 10 }, maxTicksLimit: 18 } },
      y: { grid: { color: '#F1F5F9' }, border: { display: false },
           ticks: { color: '#64748B', font: { size: 10 }, callback: (v: any) => (v / 1e6).toFixed(0) + ' M' } },
    },
  };

  readonly modelMeta  = MODEL_META;
  readonly metricInfo = METRIC_INFO;
  readonly metricKeys = Object.keys(METRIC_INFO);
  readonly Math       = Math;

  constructor(private ml: MlService) {}

  ngOnInit(): void {
    forkJoin({
      forecast:   this.ml.getForecast(6),
      dimensions: this.ml.getForecastDimensions(),
    }).subscribe({
      next: ({ forecast, dimensions }) => {
        this.result     = forecast;
        this.dimensions = dimensions;
        this.loading    = false;
        this.buildMainChart();
      },
      error: () => {
        this.error   = 'ML service unavailable. Make sure the Python API is running on port 8000.';
        this.loading = false;
      },
    });
  }

  // ── Charts ─────────────────────────────────────────────────────────────────

  buildMainChart(): void {
    const hist = (this.result?.historical || []).slice(-48);
    const fore = this.result?.forecast || [];

    const histLabels = hist.map((r: any) => r.date);
    const foreLabels = fore.map((r: any) => r.date);
    const allLabels  = [...histLabels, ...foreLabels];

    const histValues  = hist.map((r: any) => r.actual_netpay);
    const foreValues  = fore.map((r: any) => r.predicted_netpay);
    const upperValues = fore.map((r: any) => r.upper);
    const lowerValues = fore.map((r: any) => r.lower);

    const pad = (arr: any[], n: number) => [...Array(n).fill(null), ...arr];
    const padH = (arr: any[], n: number) => [...arr, ...Array(n).fill(null)];

    this.mainChartData = {
      labels: allLabels,
      datasets: [
        {
          label: 'Historical Payroll',
          data: padH(histValues, foreLabels.length),
          borderColor: '#1E3048', backgroundColor: 'rgba(30,48,72,.07)',
          borderWidth: 2, pointRadius: 0, tension: 0.3, fill: true, spanGaps: true,
        },
        {
          label: `Forecast — ${this.winnerLabel} (${this.result?.mape?.toFixed(2)}% MAPE)`,
          data: pad(foreValues, histLabels.length),
          borderColor: '#C9A84C', backgroundColor: 'rgba(201,168,76,.10)',
          borderWidth: 2.5, borderDash: [7, 4],
          pointBackgroundColor: '#C9A84C', pointRadius: 5, tension: 0.2, fill: true, spanGaps: true,
        },
        {
          label: '95% Confidence — Upper',
          data: pad(upperValues, histLabels.length),
          borderColor: 'rgba(201,168,76,.25)', backgroundColor: 'transparent',
          borderWidth: 1, borderDash: [3, 3], pointRadius: 0, tension: 0.2, spanGaps: true,
        },
        {
          label: '95% Confidence — Lower',
          data: pad(lowerValues, histLabels.length),
          borderColor: 'rgba(201,168,76,.25)', backgroundColor: 'rgba(201,168,76,.05)',
          borderWidth: 1, borderDash: [3, 3], pointRadius: 0, tension: 0.2, fill: '-1', spanGaps: true,
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

    this.filteredChartData = {
      labels: data.map((r: any) => r.date),
      datasets: [{
        label: `${filterLabel} — Historical`,
        data: data.map((r: any) => r.actual_netpay),
        borderColor: '#8B5CF6', backgroundColor: 'rgba(139,92,246,.08)',
        borderWidth: 2, pointRadius: 0, tension: 0.3, fill: true,
      }],
    };
  }

  // ── Filter actions ─────────────────────────────────────────────────────────

  onMinistryChange(): void {
    // Reset grade whenever ministry changes
    this.selectedGrade   = '';
    this.availableGrades = [];

    if (!this.selectedMinistry) {
      this.filteredData      = [];
      this.filteredChartData = { labels: [], datasets: [] };
      return;
    }

    // Load grades scoped to this ministry, then auto-apply ministry-only filter
    this.gradesLoading = true;
    this.ml.getForecastGrades(this.selectedMinistry).subscribe({
      next: (res: any) => {
        this.availableGrades = res.grades || [];
        this.gradesLoading   = false;
        this.applyFilter();   // show ministry-level chart right away
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

  // ── Helpers ────────────────────────────────────────────────────────────────

  get winnerKey():   string { return this.result?.model || ''; }
  get winnerLabel(): string { return MODEL_META[this.winnerKey]?.label || this.winnerKey.toUpperCase(); }

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

  isWinner(m: string):  boolean { return m === this.winnerKey; }
  isRidge(m: string):   boolean { return m === 'ridge'; }
  metaOf(m: string):    ModelMeta { return MODEL_META[m] || { label: m.toUpperCase(), description: '', color: '#94A3B8', type: '' }; }

  ministryName(code: string): string {
    const found = (this.dimensions?.ministries || []).find((m: any) => m.code === code);
    return found?.name || code;
  }
  gradeLabel(code: string): string {
    const found = (this.dimensions?.grades || []).find((g: any) => g.code === code);
    return found?.label || code;
  }

  fmt(n: number): string {
    if (!n) return '—';
    return (n / 1e6).toFixed(1) + ' M TND';
  }
  fmtPct(n: number): string { return n != null ? n.toFixed(2) + '%' : '—'; }
  fmtNum(n: number): string { return n ? n.toLocaleString('fr-TN', { maximumFractionDigits: 0 }) : '—'; }
  r2Color(r2: number): string {
    if (r2 > 0.5)  return '#15803D';
    if (r2 > 0)    return '#854D0E';
    return '#DC2626';
  }
}
