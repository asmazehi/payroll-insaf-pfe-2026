import { Component, OnInit } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { AuthService } from '../../services/auth.service';
import { environment } from '../../../environments/environment';

@Component({
  selector: 'app-profile',
  templateUrl: './profile.component.html',
  styleUrls: ['./profile.component.scss']
})
export class ProfileComponent implements OnInit {
  loading = true;
  saving  = false;
  success = '';
  error   = '';

  profile: any = {};

  form = {
    phone:           '',
    newPassword:     '',
    confirmPassword: ''
  };

  showNewPwd = false;

  private base = environment.apiUrl;

  constructor(private http: HttpClient, public auth: AuthService) {}

  ngOnInit(): void {
    this.http.get(`${this.base}/profile`).subscribe({
      next: (p: any) => { this.profile = p; this.form.phone = p.phone || ''; this.loading = false; },
      error: ()      => { this.loading = false; }
    });
  }

  save(): void {
    this.error = ''; this.success = '';

    const changingPassword = !!this.form.newPassword;
    if (changingPassword) {
      if (this.form.newPassword.length < 6) { this.error = 'New password must be at least 6 characters.'; return; }
      if (this.form.newPassword !== this.form.confirmPassword) { this.error = 'Passwords do not match.'; return; }
    }

    const body: any = { phone: this.form.phone };
    if (changingPassword) body.newPassword = this.form.newPassword;

    this.saving = true;
    this.http.put(`${this.base}/profile`, body).subscribe({
      next: (res: any) => {
        this.saving  = false;
        this.success = 'Profile updated successfully.';
        this.form.newPassword     = '';
        this.form.confirmPassword = '';
        if (res.passwordChanged) {
          // Update localStorage so the banner disappears immediately
          this.auth.markPasswordChanged();
        }
      },
      error: (err) => {
        this.saving = false;
        this.error  = err?.error?.error || 'Update failed.';
      }
    });
  }

  initials(): string {
    return (this.profile.username || 'U').slice(0, 2).toUpperCase();
  }
}
