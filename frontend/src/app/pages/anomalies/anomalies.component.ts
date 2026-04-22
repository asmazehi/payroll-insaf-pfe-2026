import { Component, OnInit } from '@angular/core';
import { MlService } from '../../services/ml.service';

@Component({
  selector: 'app-anomalies',
  templateUrl: './anomalies.component.html',
  styleUrls: ['./anomalies.component.scss']
})
export class AnomaliesComponent implements OnInit {
  loading  = true;
  result: any = null;
  error    = '';
  columns  = ['employee_sk', 'grade_code', 'ministry_code', 'year_num', 'month_num', 'm_netpay', 'z_score', 'anomaly_flag'];

  constructor(private ml: MlService) {}

  ngOnInit(): void {
    this.ml.getAnomalies(100).subscribe({
      next: (res: any) => { this.result = res; this.loading = false; },
      error: () => { this.error = 'ML service unavailable'; this.loading = false; }
    });
  }
}
