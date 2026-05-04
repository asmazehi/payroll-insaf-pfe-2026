import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export interface UserDto {
  id: number;
  username: string;
  email: string;
  role: string;
  ministryCode: string | null;
  enabled: boolean;
}

export interface EtlJob {
  id: number;
  runId: string;
  fileName: string;
  fileType: string;
  status: string;
  startedAt: string;
  finishedAt: string | null;
  rowsWritten: number | null;
  qgStatus: string | null;
  errorDetail: string | null;
  uploadedBy: string | null;
}

@Injectable({ providedIn: 'root' })
export class AdminService {
  private base = environment.apiUrl;

  constructor(private http: HttpClient) {}

  getUsers(): Observable<UserDto[]> {
    return this.http.get<UserDto[]>(`${this.base}/admin/users`);
  }

  createUser(payload: Partial<UserDto> & { password: string }): Observable<UserDto> {
    return this.http.post<UserDto>(`${this.base}/admin/users`, payload);
  }

  updateUser(id: number, payload: Partial<UserDto> & { password?: string }): Observable<UserDto> {
    return this.http.put<UserDto>(`${this.base}/admin/users/${id}`, payload);
  }

  toggleUser(id: number): Observable<{ enabled: boolean }> {
    return this.http.put<{ enabled: boolean }>(`${this.base}/admin/users/${id}/toggle`, {});
  }

  deleteUser(id: number): Observable<any> {
    return this.http.delete(`${this.base}/admin/users/${id}`);
  }

  getEtlJobs(): Observable<EtlJob[]> {
    return this.http.get<EtlJob[]>(`${this.base}/etl/jobs`);
  }
}
