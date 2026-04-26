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
    animation: { duration: 800, easing: 'easeOutQuart' },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: 'rgba(13,27,42,.92)',
        titleColor: '#E8C96A',
        bodyColor: '#fff',
        padding: 12,
        cornerRadius: 10,
        callbacks: { label: (ctx) => '  ' + this.fmt(ctx.parsed.y ?? 0) }
      }
    },
    scales: {
      x: { grid: { display: false }, ticks: { color: '#6B7C93', font: { size: 11 } }, border: { display: false } },
      y: { grid: { color: '#F1F5F9' }, border: { display: false },
           ticks: { color: '#6B7C93', font: { size: 11 }, callback: (v: any) => (v/1e6).toFixed(0) + 'M' } }
    }
  };

  ministryData: ChartData<'doughnut'> = { labels: [], datasets: [] };
  doughnutOptions: ChartConfiguration['options'] = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 800, easing: 'easeOutQuart' },
    plugins: {
      legend: { position: 'right', labels: { font: { size: 11, family: 'Inter' }, boxWidth: 14, padding: 14, color: '#6B7C93' } },
      tooltip: {
        backgroundColor: 'rgba(13,27,42,.92)',
        titleColor: '#E8C96A',
        bodyColor: '#fff',
        padding: 12, cornerRadius: 10
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
          backgroundColor: rows.map((_, i) => i === last ? '#C9A84C' : 'rgba(13,27,42,.75)'),
          hoverBackgroundColor: rows.map((_, i) => i === last ? '#E8C96A' : '#1E3A5F'),
          borderRadius: 8,
          borderSkipped: false,
          label: 'Net Payroll'
        }]
      };
    });

    this.svc.getByMinistry().subscribe(rows => {
      this.byMinistry = rows.slice(0, 8);
      const colors = ['#0D1B2A','#C9A84C','#1E3A5F','#3B82F6','#10B981','#7C3AED','#F59E0B','#EF4444'];
      this.ministryData = {
        labels: rows.slice(0, 8).map(r => r['ministry']),
        datasets: [{
          data: rows.slice(0, 8).map(r => r['total_netpay']),
          backgroundColor: colors,
          hoverOffset: 8,
          borderWidth: 2,
          borderColor: '#fff'
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
    return (n / 1e6).toFixed(1) + ' M';
  }
}
