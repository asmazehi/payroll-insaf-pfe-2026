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

  barData: ChartData<'bar'> = { labels: [], datasets: [] };
  barOptions: ChartConfiguration['options'] = {
    responsive: true,
    plugins: { legend: { display: false },
      title: { display: true, text: 'Total Net Payroll by Year (TND)' } }
  };

  constructor(private svc: DashboardService) {}

  ngOnInit(): void {
    this.svc.getSummary().subscribe(s => { this.summary = s; this.loading = false; });
    this.svc.getPayrollByYear().subscribe(rows => {
      this.barData = {
        labels: rows.map(r => r['year_num']),
        datasets: [{ data: rows.map(r => r['total_netpay']), backgroundColor: '#3f51b5', label: 'Net Payroll' }]
      };
    });
  }

  fmt(n: number): string {
    return n ? (n / 1_000_000).toFixed(2) + ' M TND' : '—';
  }
}
