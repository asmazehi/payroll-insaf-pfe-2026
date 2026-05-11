import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

@Injectable({ providedIn: 'root' })
export class MlService {
  private base = `${environment.apiUrl}/ml`;

  constructor(private http: HttpClient) {}

  getForecast(n = 6): Observable<any>         { return this.http.get(`${this.base}/forecast?n=${n}`); }
  getForecastDimensions(): Observable<any>    { return this.http.get(`${this.base}/forecast/dimensions`); }
  getForecastGrades(ministry: string): Observable<any> {
    return this.http.get(`${this.base}/forecast/dimensions?ministry=${encodeURIComponent(ministry)}`);
  }
  getForecastHistorical(ministry?: string, grade?: string): Observable<any> {
    let params = '';
    if (ministry) params += `?ministry=${encodeURIComponent(ministry)}`;
    if (grade)    params += `${params ? '&' : '?'}grade=${encodeURIComponent(grade)}`;
    return this.http.get(`${this.base}/forecast/historical${params}`);
  }
  getAnomalies(limit = 50): Observable<any>   { return this.http.get(`${this.base}/anomalies?limit=${limit}`); }
  chat(question: string): Observable<any>     { return this.http.post(`${this.base}/chat`, { question }); }
  getStatus(): Observable<any>                { return this.http.get(`${this.base}/status`); }
}
