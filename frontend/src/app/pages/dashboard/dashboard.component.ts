import { Component, OnInit } from '@angular/core';
import { DashboardService } from '../../services/dashboard.service';
import { ChartConfiguration, ChartData } from 'chart.js';

@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements OnInit {
  summary: any = null;
  loading = true;
  byMinistry: any[] = [];
  byGrade: any[] = [];

  barData: ChartData<'bar'> = { labels: [], datasets: [] };
  barOptions: ChartConfiguration['options'] = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 900, easing: 'easeOutQuart' },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: 'rgba(13,13,31,0.95)',
        titleColor: 'rgba(255,255,255,0.55)',
        bodyColor: 'rgba(255,255,255,0.92)',
        borderColor: 'rgba(255,255,255,0.1)',
        borderWidth: 1,
        padding: 14,
        cornerRadius: 10,
        callbacks: { label: (ctx) => '  ' + this.fmt(ctx.parsed.y ?? 0) }
      }
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: { color: 'rgba(255,255,255,0.25)', font: { size: 11, family: 'JetBrains Mono' } },
        border: { display: false }
      },
      y: {
        grid: { color: 'rgba(255,255,255,0.04)', drawTicks: false },
        border: { display: false },
        ticks: { color: 'rgba(255,255,255,0.25)', font: { size: 11, family: 'JetBrains Mono' }, padding: 8, callback: (v: any) => (v/1e6).toFixed(0) + 'M' }
      }
    }
  };

  ministryData: ChartData<'doughnut'> = { labels: [], datasets: [] };
  doughnutOptions: ChartConfiguration['options'] = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 700, easing: 'easeOutQuart' },
    plugins: {
      legend: {
        position: 'right',
        labels: { font: { size: 11, family: 'Inter' }, boxWidth: 10, padding: 12, color: 'rgba(255,255,255,0.45)' }
      },
      tooltip: {
        backgroundColor: '#141422',
        titleColor: 'rgba(255,255,255,0.55)',
        bodyColor: 'rgba(255,255,255,0.92)',
        borderColor: 'rgba(255,255,255,0.08)',
        borderWidth: 1,
        padding: 12,
        cornerRadius: 8
      }
    }
  };

  constructor(private svc: DashboardService) {}

  ngOnInit(): void {
    this.svc.getSummary().subscribe(s => { this.summary = s; this.loading = false; });

    this.svc.getPayrollByYear().subscribe(rows => {
      const last = rows.length - 1;
      this.barData = {
        labels: rows.map(r => r['year_num']),
        datasets: [{
          data: rows.map(r => r['total_netpay']),
          backgroundColor: rows.map((_, i) => i === last ? '#6366F1' : 'rgba(99,102,241,0.25)'),
          hoverBackgroundColor: rows.map((_, i) => i === last ? '#818CF8' : 'rgba(99,102,241,0.45)'),
          borderRadius: 8,
          borderSkipped: false,
          label: 'Net Payroll'
        }]
      };
    });

    this.svc.getByMinistry().subscribe(rows => {
      this.byMinistry = rows.slice(0, 8);
      const colors = ['#6366F1','#06B6D4','#10B981','#F59E0B','#8B5CF6','#EF4444','#EC4899','#14B8A6'];
      this.ministryData = {
        labels: rows.slice(0, 8).map(r => r['ministry']),
        datasets: [{
          data: rows.slice(0, 8).map(r => r['total_netpay']),
          backgroundColor: colors,
          hoverOffset: 6,
          borderWidth: 2,
          borderColor: '#0E0E1A'
        }]
      };
    });

    this.svc.getByGrade().subscribe(rows => { this.byGrade = rows.slice(0, 8); });
  }

  fmt(n: number): string {
    if (!n) return '—';
    if (n >= 1e9) return (n / 1e9).toFixed(2) + ' B TND';
    if (n >= 1e6) return (n / 1e6).toFixed(2) + ' M TND';
    return n.toFixed(0) + ' TND';
  }

  fmtM(n: number): string {
    if (!n) return '—';
    return (n / 1e6).toFixed(1);
  }

  fmtCompact(n: number): string {
    if (!n) return '—';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(0) + 'K';
    return n.toFixed(0);
  }
}
