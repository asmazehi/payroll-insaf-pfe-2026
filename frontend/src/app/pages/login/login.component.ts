import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { HttpClient } from '@angular/common/http';
import { AuthService } from '../../services/auth.service';
import { LangService, Lang } from '../../services/lang.service';
import { environment } from '../../../environments/environment';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss']
})
export class LoginComponent implements OnInit {
  form: FormGroup;
  loading = false;
  error   = '';
  locked  = false;
  showPwd = false;

  showForgot    = false;
  forgotEmail   = '';
  forgotLoading = false;
  forgotSuccess = false;
  forgotError   = '';

  stats = { total_employees: 0, total_records: 0, years_of_data: 0 };
  statsLoaded = false;

  // Animated display values (count-up effect)
  displayEmployees = 0;
  displayRecords   = 0;
  displayYears     = 0;

  constructor(
    private fb:   FormBuilder,
    private auth: AuthService,
    private router: Router,
    private http:   HttpClient,
    public  lang:   LangService
  ) {
    this.form = this.fb.group({
      username: ['', Validators.required],
      password: ['', Validators.required]
    });
  }

  ngOnInit(): void {
    this.http.get<any>(`${environment.apiUrl}/public/stats`).subscribe({
      next: (s) => {
        this.stats = s;
        this.statsLoaded = true;
        this._countUp('displayEmployees', s.total_employees, 1800);
        this._countUp('displayRecords',   s.total_records,   2000);
        this._countUp('displayYears',     s.years_of_data,   800);
      },
      error: () => {
        // fallback values if API unreachable (login page shown before auth)
        this.stats = { total_employees: 25000, total_records: 760000, years_of_data: 10 };
        this.statsLoaded = true;
        this._countUp('displayEmployees', this.stats.total_employees, 1800);
        this._countUp('displayRecords',   this.stats.total_records,   2000);
        this._countUp('displayYears',     this.stats.years_of_data,   800);
      }
    });
  }

  private _countUp(field: 'displayEmployees' | 'displayRecords' | 'displayYears',
                   target: number, duration: number): void {
    const steps = 60;
    const step  = target / steps;
    let  current = 0;
    const interval = setInterval(() => {
      current += step;
      if (current >= target) { (this as any)[field] = target; clearInterval(interval); }
      else                   { (this as any)[field] = Math.round(current); }
    }, duration / steps);
  }

  fmtStat(n: number): string {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M+';
    if (n >= 1_000)     return (n / 1_000).toFixed(0) + 'K+';
    return n.toString() + '+';
  }

  setLang(code: Lang): void { this.lang.setLang(code); }

  openForgot(): void {
    this.showForgot  = true;
    this.forgotEmail = '';
    this.forgotSuccess = false;
    this.forgotError   = '';
  }

  closeForgot(): void {
    this.showForgot = false;
  }

  submitForgot(): void {
    if (!this.forgotEmail.trim()) return;
    this.forgotLoading = true;
    this.forgotError   = '';
    this.http.post(`${environment.apiUrl}/auth/forgot-password`, { email: this.forgotEmail }).subscribe({
      next: () => { this.forgotLoading = false; this.forgotSuccess = true; },
      error: () => { this.forgotLoading = false; this.forgotError = 'Une erreur est survenue. Veuillez réessayer.'; }
    });
  }

  submit(): void {
    if (this.form.invalid) return;
    this.loading = true;
    this.error   = '';
    this.locked  = false;
    const { username, password } = this.form.value;
    this.auth.login(username, password).subscribe({
      next: () => this.router.navigate(['/dashboard']),
      error: (err) => {
        this.loading = false;
        const status = err?.status;
        if (status === 429) {
          this.locked = true;
          this.error  = err?.error?.error || 'Account temporarily locked. Try again later.';
        } else {
          this.error = 'Invalid username or password.';
        }
      }
    });
  }
}
