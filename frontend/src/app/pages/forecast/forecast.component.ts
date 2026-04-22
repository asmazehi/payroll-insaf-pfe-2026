import { Component, OnInit } from '@angular/core';
import { MlService } from '../../services/ml.service';
import { ChartData, ChartConfiguration } from 'chart.js';

@Component({
  selector: 'app-forecast',
  templateUrl: './forecast.component.html',
  styleUrls: ['./forecast.component.scss']
})
export class ForecastComponent implements OnInit {
  loading  = true;
  result: any = null;
  error    = '';

  lineData: ChartData<'line'> = { labels: [], datasets: [] };
  lineOptions: ChartConfiguration['options'] = {
    responsive: true,
    plugins: { title: { display: true, text: '6-Month Payroll Forecast (TND)' } }
  };

  constructor(private ml: MlService) {}

  ngOnInit(): void {
    this.ml.getForecast(6).subscribe({
      next: (res: any) => {
        this.result  = res;
        this.loading = false;
        const rows   = res.forecast || [];
        this.lineData = {
          labels: rows.map((r: any) => r.date),
          datasets: [{
            data: rows.map((r: any) => r.predicted_netpay),
            borderColor: '#3f51b5', backgroundColor: 'rgba(63,81,181,0.1)',
            tension: 0.4, fill: true, label: 'Predicted Net Payroll'
          }]
        };
      },
      error: () => { this.error = 'ML service unavailable'; this.loading = false; }
    });
  }
}
