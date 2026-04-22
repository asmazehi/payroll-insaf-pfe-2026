import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

@Injectable({ providedIn: 'root' })
export class DashboardService {
  private base = `${environment.apiUrl}/dashboard`;

  constructor(private http: HttpClient) {}

  getSummary(): Observable<any>              { return this.http.get(`${this.base}/summary`); }
  getPayrollByYear(): Observable<any[]>      { return this.http.get<any[]>(`${this.base}/payroll-by-year`); }
  getPayrollByMonth(year: number): Observable<any[]> {
    return this.http.get<any[]>(`${this.base}/payroll-by-month?year=${year}`);
  }
  getByGrade(): Observable<any[]>            { return this.http.get<any[]>(`${this.base}/by-grade`); }
  getByMinistry(): Observable<any[]>         { return this.http.get<any[]>(`${this.base}/by-ministry`); }
  getIndemnitySummary(): Observable<any>     { return this.http.get(`${this.base}/indemnity-summary`); }
}
