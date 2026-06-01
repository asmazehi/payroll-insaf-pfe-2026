import { Component, OnInit } from '@angular/core';
import { DashboardService } from '../../services/dashboard.service';
import { AuthService } from '../../services/auth.service';
import { ChartConfiguration, ChartData } from 'chart.js';

@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements OnInit {
  summary: any  = null;
  loading       = true;
  byMinistry: any[] = [];
  byGrade:    any[] = [];

  greeting    = '';
  greetingIcon = '';

  barData:    ChartData<'bar'> = { labels: [], datasets: [] };
  barOptions: ChartConfiguration['options'] = {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    animation: { duration: 900, easing: 'easeOutQuart' },
    plugins: {
      legend: {
        display: true, position: 'top',
        labels: { color: 'rgba(255,255,255,.45)', font: { size: 11 }, boxWidth: 10, padding: 16 }
      },
      tooltip: {
        backgroundColor: 'rgba(10,10,30,.97)',
        titleColor: 'rgba(255,255,255,.6)', bodyColor: '#fff',
        borderColor: 'rgba(255,255,255,.08)', borderWidth: 1,
        padding: 14, cornerRadius: 10,
        callbacks: {
          label: (ctx) => {
            const v = ctx.parsed.y ?? 0;
            if (v >= 1e9) return `  ${(v/1e9).toFixed(2)} B TND`;
            if (v >= 1e6) return `  ${(v/1e6).toFixed(2)} M TND`;
            return `  ${v.toFixed(0)} TND`;
          }
        }
      }
    },
    scales: {
      x: {
        grid: { display: false }, border: { display: false },
        ticks: { color: 'rgba(255,255,255,.2)', font: { size: 11 } }
      },
      y: {
        grid: { color: 'rgba(255,255,255,.04)', drawTicks: false },
        border: { display: false },
        ticks: {
          color: 'rgba(255,255,255,.2)', font: { size: 11 }, padding: 8,
          callback: (v: any) => (v / 1e6).toFixed(0) + 'M'
        }
      }
    }
  };

  ministryData:    ChartData<'doughnut'> = { labels: [], datasets: [] };
  doughnutOptions: ChartConfiguration['options'] = {
    responsive: true, maintainAspectRatio: false,
    animation: { duration: 700, easing: 'easeOutQuart' },
    plugins: {
      legend: {
        position: 'right',
        labels: { font: { size: 11 }, boxWidth: 10, padding: 14, color: 'rgba(255,255,255,.45)' }
      },
      tooltip: {
        backgroundColor: 'rgba(10,10,30,.97)',
        titleColor: 'rgba(255,255,255,.6)', bodyColor: '#fff',
        borderColor: 'rgba(255,255,255,.08)', borderWidth: 1,
        padding: 12, cornerRadius: 8
      }
    }
  };

  constructor(private svc: DashboardService, public auth: AuthService) {}

  get isAdmin(): boolean { return this.auth.isAdmin(); }
  get username(): string { return this.auth.getCurrentUser()?.username || 'User'; }
  get today(): string {
    return new Date().toLocaleDateString('en-GB', { weekday:'long', day:'numeric', month:'long', year:'numeric' });
  }

  ngOnInit(): void {
    this._setGreeting();

    this.svc.getSummary().subscribe(s => { this.summary = s; this.loading = false; });

    this.svc.getPayrollByYear().subscribe(rows => {
      const n = rows.length;
      this.barData = {
        labels: rows.map(r => r['year_num']),
        datasets: [
          {
            label: 'Net Payroll',
            data:  rows.map(r => r['total_netpay']),
            backgroundColor: (ctx: any) => {
              const canvas = ctx.chart.ctx;
              const grad = canvas.createLinearGradient(0, 0, 0, 320);
              grad.addColorStop(0, 'rgba(99,102,241,0.9)');
              grad.addColorStop(1, 'rgba(99,102,241,0.3)');
              return grad;
            },
            borderColor: '#818cf8',
            borderWidth: 1,
            borderRadius: 5,
            borderSkipped: false,
          } as any
        ]
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
          hoverOffset: 8,
          borderWidth: 2,
          borderColor: '#0E0E1A'
        }]
      };
    });

    this.svc.getByGrade().subscribe(rows => { this.byGrade = rows.slice(0, 8); });
  }

  private _setGreeting(): void {
    const h = new Date().getHours();
    if (h < 12)      { this.greeting = 'Good morning';    this.greetingIcon = '🌅'; }
    else if (h < 17) { this.greeting = 'Good afternoon';  this.greetingIcon = '☀️'; }
    else             { this.greeting = 'Good evening';    this.greetingIcon = '🌙'; }
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

  gradeBar(val: number): number {
    if (!this.byGrade.length) return 0;
    const max = this.byGrade[0]?.avg_netpay || 1;
    return Math.round((val / max) * 100);
  }
}
