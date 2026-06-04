import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';
import { AuthService } from './auth.service';

@Injectable({ providedIn: 'root' })
export class MlService {
  private base = `${environment.apiUrl}/ml`;

  constructor(private http: HttpClient, private auth: AuthService) {}

  getForecast(n = 6): Observable<any>          { return this.http.get(`${this.base}/forecast?n=${n}`); }
  getFeatureImportance(): Observable<any>      { return this.http.get(`${this.base}/forecast/feature-importance`); }
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
  getEmployeeForecast(employeeId: string): Observable<any> {
    return this.http.get(`${this.base}/forecast/employee?employee_id=${encodeURIComponent(employeeId)}`);
  }
  getAnomalies(limit = 50, lang = 'en'): Observable<any> { return this.http.get(`${this.base}/anomalies?limit=${limit}&lang=${lang}`); }
  getAnomaliesByMinistry(): Observable<any>            { return this.http.get(`${this.base}/anomalies/by-ministry`); }
  getAnomaliesByGrade(): Observable<any>               { return this.http.get(`${this.base}/anomalies/by-grade`); }
  getAnomalyTemporalContext(empSk: number, year: number, month: number): Observable<any> {
    return this.http.get(`${this.base}/anomalies/temporal-context?employee_sk=${empSk}&year_num=${year}&month_num=${month}`);
  }
  chat(question: string, history: {role: string, text: string}[] = []): Observable<any> {
    const user = this.auth.getCurrentUser();
    const ministry_code = (!user || user.role === 'ROLE_ADMIN') ? null : (user.ministryCode ?? null);
    return this.http.post(`${this.base}/chat`, { question, history, ministry_code });
  }

  chatStream(question: string, history: {role: string, text: string}[] = []):
      Observable<{token: string, done: boolean, entities?: any}> {
    const user         = this.auth.getCurrentUser();
    const ministry_code = (!user || user.role === 'ROLE_ADMIN') ? null : (user.ministryCode ?? null);
    const token        = localStorage.getItem('insaf_token') ?? '';
    const body         = JSON.stringify({ question, history, ministry_code, model: 'llama3.2:1b' });

    return new Observable(observer => {
      fetch(`${this.base}/chat/stream`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body,
      }).then(res => {
        const reader  = res.body!.getReader();
        const decoder = new TextDecoder();
        let   buffer  = '';

        const pump = (): any => reader.read().then(({ done, value }) => {
          if (done) { observer.complete(); return; }

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() ?? '';          // keep incomplete last line

          for (const line of lines) {
            if (!line.startsWith('data: ')) continue;
            try {
              const data = JSON.parse(line.slice(6));
              observer.next(data);
              if (data.done) { observer.complete(); return; }
            } catch {}
          }
          return pump();
        });
        pump().catch((e: any) => observer.error(e));
      }).catch(e => observer.error(e));

      return () => {};
    });
  }
  getStatus(): Observable<any>                { return this.http.get(`${this.base}/status`); }

  submitReview(employeeSk: number, yearNum: number, monthNum: number, status: string, notes?: string): Observable<any> {
    return this.http.post(`${environment.apiUrl}/anomalies/reviews`,
      { employee_sk: employeeSk, year_num: yearNum, month_num: monthNum, status, notes: notes || null });
  }

  removeReview(employeeSk: number, yearNum: number, monthNum: number): Observable<any> {
    return this.http.delete(`${environment.apiUrl}/anomalies/reviews`,
      { body: { employee_sk: employeeSk, year_num: yearNum, month_num: monthNum } });
  }

  dismissAnomaly(employeeSk: number, yearNum: number, monthNum: number): Observable<any> {
    return this.http.post(`${environment.apiUrl}/anomalies/reviews/dismiss`,
      { employee_sk: employeeSk, year_num: yearNum, month_num: monthNum });
  }

  restoreAnomaly(employeeSk: number, yearNum: number, monthNum: number): Observable<any> {
    return this.http.post(`${environment.apiUrl}/anomalies/reviews/restore`,
      { employee_sk: employeeSk, year_num: yearNum, month_num: monthNum });
  }

  getDismissed(): Observable<any> {
    return this.http.get(`${environment.apiUrl}/anomalies/reviews/dismissed`);
  }
}
