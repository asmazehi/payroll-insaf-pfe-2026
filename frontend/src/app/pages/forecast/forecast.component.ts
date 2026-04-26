import { Component, OnInit } from '@angular/core';
import { MlService } from '../../services/ml.service';
import { ChartData, ChartConfiguration } from 'chart.js';

@Component({
  selector: 'app-forecast',
  templateUrl: './forecast.component.html',
  styleUrls: ['./forecast.component.scss']
})
export class ForecastComponent implements OnInit {
  loading = true;
  result: any = null;
  error = '';

  lineData: ChartData<'line'> = { labels: [], datasets: [] };
  lineOptions: ChartConfiguration['options'] = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    animation: { duration: 900, easing: 'easeOutQuart' },
    plugins: {
      legend: {
        display: true, position: 'top',
        labels: { font: { size: 11, family: 'Inter' }, boxWidth: 14, padding: 16, color: '#64748B' }
      },
      tooltip: {
        backgroundColor: 'rgba(13,27,42,.92)',
        titleColor: '#E8C96A', bodyColor: '#fff',
        padding: 14, cornerRadius: 12,
        callbacks: { label: (c) => `  ${c.dataset.label}: ${Number(c.parsed.y).toLocaleString('fr-TN', {maximumFractionDigits: 0})} TND` }
      }
    },
    scales: {
      x: { grid: { display: false }, border: { display: false }, ticks: { color: '#64748B', font: { size: 11 } } },
      y: { grid: { color: '#F1F5F9' }, border: { display: false },
           ticks: { color: '#64748B', font: { size: 11 }, callback: (v: any) => (v/1e6).toFixed(1) + 'M' } }
    }
  };

  constructor(private ml: MlService) {}

  ngOnInit(): void {
    this.ml.getForecast(6).subscribe({
      next: (res: any) => {
        this.result  = res;
        this.loading = false;
        this.buildChart(res);
      },
      error: () => { this.error = 'ML service unavailable. Please start the Python API.'; this.loading = false; }
    });
  }

  buildChart(res: any): void {
    const hist = res.historical || [];
    const fore = res.forecast   || [];

    const histLabels = hist.map((r: any) => r.date);
    const foreLabels = fore.map((r: any) => r.date);
    const allLabels  = [...histLabels, ...foreLabels];

    const histValues = hist.map((r: any) => r.actual_netpay ?? r.netpay ?? null);
    const foreValues = fore.map((r: any) => r.predicted_netpay);

    // Pad historical data with nulls for forecast period
    const histPadded = [...histValues, ...Array(foreLabels.length).fill(null)];
    const forePadded = [...Array(histLabels.length).fill(null), ...foreValues];

    this.lineData = {
      labels: allLabels,
      datasets: [
        {
          label: 'Historical',
          data: histPadded,
          borderColor: '#1E3048',
          backgroundColor: 'rgba(30,48,72,.05)',
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.3,
          fill: true,
          spanGaps: true
        },
        {
          label: `Forecast (${res.model?.toUpperCase() || 'ML'})`,
          data: forePadded,
          borderColor: '#C9A84C',
          backgroundColor: 'rgba(201,168,76,.12)',
          borderWidth: 2.5,
          borderDash: [6, 3],
          pointBackgroundColor: '#C9A84C',
          pointRadius: 5,
          tension: 0.3,
          fill: true,
          spanGaps: true
        }
      ]
    };
  }

  get allMetrics(): any[] {
    return Object.entries(this.result?.all_metrics || {}).map(([model, m]: any) => ({
      model, ...m
    }));
  }

  modelLabel(m: string): string {
    const map: any = { rf: 'Random Forest', xgb: 'XGBoost', lr: 'Linear Reg.',
      arima: 'ARIMA', prophet: 'Prophet', tft: 'TFT (Deep)' };
    return map[m] || m.toUpperCase();
  }

  isWinner(m: string): boolean { return m === this.result?.model; }

  fmt(n: number): string {
    if (!n) return '—';
    return (n / 1e6).toFixed(2) + ' M TND';
  }
}
