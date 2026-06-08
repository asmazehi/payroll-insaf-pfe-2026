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
  ministryName: string | null;
  phone: string | null;
  profession: string | null;
  profilePhoto: string | null;
  enabled: boolean;
}

export interface MinistryOption {
  code: string;
  name: string;       // French label
  name_ar?: string;   // Arabic label
}

export interface Ticket {
  id: number;
  title: string;
  description: string | null;
  status: string;
  ministryCode: string | null;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
  resolvedAt: string | null;
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

  getMinistries(): Observable<MinistryOption[]> {
    return this.http.get<MinistryOption[]>(`${this.base}/admin/users/ministries`);
  }

  getEstablishments(ministryCode: string): Observable<MinistryOption[]> {
    return this.http.get<MinistryOption[]>(
      `${this.base}/admin/users/establishments?ministry=${encodeURIComponent(ministryCode)}`
    );
  }

  getParentMinistry(code: string): Observable<{ parentMinistry: string; code: string }> {
    return this.http.get<{ parentMinistry: string; code: string }>(
      `${this.base}/admin/users/parent-ministry?code=${encodeURIComponent(code)}`
    );
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

  // Tickets
  getTickets(): Observable<Ticket[]> {
    return this.http.get<Ticket[]>(`${this.base}/tickets`);
  }

  createTicket(title: string, description: string): Observable<Ticket> {
    return this.http.post<Ticket>(`${this.base}/tickets`, { title, description });
  }

  editTicket(id: number, title: string, description: string): Observable<Ticket> {
    return this.http.put<Ticket>(`${this.base}/tickets/${id}`, { title, description });
  }

  updateTicketStatus(id: number, status: string): Observable<Ticket> {
    return this.http.put<Ticket>(`${this.base}/tickets/${id}/status`, { status });
  }

  deleteTicket(id: number): Observable<any> {
    return this.http.delete(`${this.base}/tickets/${id}`);
  }

  getOpenTicketCount(): Observable<{ count: number }> {
    return this.http.get<{ count: number }>(`${this.base}/tickets/count/open`);
  }
}
