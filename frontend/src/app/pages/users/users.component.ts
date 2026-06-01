import { Component, OnInit } from '@angular/core';
import { AdminService, UserDto, MinistryOption } from '../../services/admin.service';
import { AuthService } from '../../services/auth.service';
import { LangService } from '../../services/lang.service';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../../environments/environment';

@Component({
  selector: 'app-users',
  templateUrl: './users.component.html',
  styleUrls: ['./users.component.scss']
})
export class UsersComponent implements OnInit {
  users: UserDto[] = [];
  admins: UserDto[] = [];
  ministries: MinistryOption[] = [];
  establishments: MinistryOption[] = [];
  loadingEstablishments = false;
  loading = true;
  error = '';

  showForm = false;
  editingUser: UserDto | null = null;

  // selectedMinistry holds the top-level ministry; form.ministryCode holds the final codetab
  // (either the ministry itself if no specific establishment is chosen, or the establishment)
  selectedMinistry = '';

  form = {
    username: '',
    email: '',
    password: '',
    role: 'ROLE_USER',
    ministryCode: '',
    phone: '',
    profession: '',
    profilePhoto: ''
  };

  saving = false;
  saveError = '';

  confirmDialog: { title: string; message: string; action: () => void } | null = null;


  constructor(
    private adminService: AdminService,
    public auth: AuthService,
    public lang: LangService,
    private http: HttpClient
  ) {}

  isAdmin(): boolean { return this.auth.isAdmin(); }

  ngOnInit(): void {
    this.load();
    if (this.isAdmin()) {
      this.adminService.getMinistries().subscribe({ next: (m) => this.ministries = m, error: () => {} });
    }
  }

  load(): void {
    this.loading = true;
    this.adminService.getUsers().subscribe({
      next: (users) => {
        this.users = users;
        this.admins = users.filter(u => u.role === 'ROLE_ADMIN');
        this.loading = false;
      },
      error: (err) => { this.error = err?.error?.error || 'Failed to load users'; this.loading = false; }
    });
  }

  openCreate(): void {
    this.editingUser = null;
    this.selectedMinistry = '';
    this.establishments = [];
    this.form = { username: '', email: '', password: '', role: 'ROLE_USER',
                  ministryCode: '', phone: '', profession: '', profilePhoto: '' };
    this.saveError = '';
    this.showForm = true;
  }

  openEdit(user: UserDto): void {
    this.editingUser = user;
    const code = user.ministryCode ?? '';
    this.selectedMinistry = '';
    this.establishments = [];
    this.form = {
      username:     user.username,
      email:        user.email,
      password:     '',
      role:         user.role,
      ministryCode: code,
      phone:        user.phone ?? '',
      profession:   user.profession ?? '',
      profilePhoto: user.profilePhoto ?? ''
    };
    this.saveError = '';
    this.showForm = true;

    if (code && this.isAdmin()) {
      // Ask the backend for the parent ministry in a single call, then load establishments.
      // This avoids both the race-condition (ministries list may not be loaded yet)
      // and the N-calls approach.
      this.adminService.getParentMinistry(code).subscribe({
        next: (res) => {
          this.selectedMinistry = res.parentMinistry;
          this._loadEstablishments(res.parentMinistry, code);
        },
        error: () => {
          // Fallback: treat the code itself as the ministry
          this.selectedMinistry = code;
          this._loadEstablishments(code, code);
        }
      });
    }
  }

  onMinistryChange(): void {
    const m = this.selectedMinistry;
    this.form.ministryCode = m;
    this.establishments = [];
    if (m) this._loadEstablishments(m, m);
  }

  private _loadEstablishments(ministryCode: string, keepCode: string): void {
    this.loadingEstablishments = true;
    this.adminService.getEstablishments(ministryCode).subscribe({
      next: (list) => {
        this.establishments = list;
        this.loadingEstablishments = false;
        // Restore the specific establishment if it exists in the list, otherwise use the ministry
        this.form.ministryCode = list.some(e => e.code === keepCode) ? keepCode : ministryCode;
      },
      error: () => {
        this.loadingEstablishments = false;
        this.form.ministryCode = keepCode || ministryCode;
      }
    });
  }

  onUsernameChange(): void {
    if (!this.editingUser && this.form.username.trim()) {
      this.form.email = `${this.form.username.toLowerCase().replace(/\s+/g, '.')}.insaf@gmail.com`;
    }
  }

  onPhotoChange(event: Event): void {
    const file = (event.target as HTMLInputElement).files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => { this.form.profilePhoto = reader.result as string; };
    reader.readAsDataURL(file);
  }

  save(): void {
    this.saveError = '';
    if (this.form.role !== 'ROLE_ADMIN') {
      if (!this.selectedMinistry) {
        this.saveError = 'Please select a ministry.';
        return;
      }
      if (!this.form.ministryCode) {
        this.saveError = 'Please select a center / establishment.';
        return;
      }
    }
    this.saving = true;
    const payload: any = {
      email:        this.form.email,
      role:         this.form.role,
      ministryCode: this.form.ministryCode || null,
      phone:        this.form.phone || null,
      profession:   this.form.profession || null,
      profilePhoto: this.form.profilePhoto || null
    };
    if (!this.editingUser) payload.username = this.form.username;
    if (this.form.password) payload.password = this.form.password;

    const req = this.editingUser
      ? this.adminService.updateUser(this.editingUser.id, payload)
      : this.adminService.createUser({ ...payload, password: this.form.password });

    const previousMinistry = this.editingUser?.ministryCode ?? null;
    req.subscribe({
      next: () => {
        this.saving = false;
        this.showForm = false;
        this.load();
        // If the admin edited their own account and changed their ministry,
        // the JWT still carries the old ministry code — force re-login.
        const currentUser = this.auth.getCurrentUser();
        const editedSelf = this.editingUser && currentUser?.username === this.editingUser.username;
        const ministryChanged = this.form.ministryCode !== previousMinistry;
        if (editedSelf && ministryChanged) {
          setTimeout(() => this.auth.logout(), 800);
        }
      },
      error: (err) => { this.saving = false; this.saveError = err?.error?.error || 'Save failed'; }
    });
  }

  toggle(user: UserDto): void {
    this.adminService.toggleUser(user.id).subscribe({
      next: (res) => { user.enabled = res.enabled; },
      error: () => {}
    });
  }

  delete(user: UserDto): void {
    this.confirmDialog = {
      title: 'Delete User',
      message: `Remove <strong>${user.username}</strong> permanently? This cannot be undone.`,
      action: () => {
        this.adminService.deleteUser(user.id).subscribe({ next: () => this.load(), error: () => {} });
      }
    };
  }

  confirmYes(): void {
    this.confirmDialog?.action();
    this.confirmDialog = null;
  }

  confirmNo(): void { this.confirmDialog = null; }

  // Pick FR or AR label depending on active language
  labelOf(m: MinistryOption): string {
    return this.lang.current === 'ar' && m.name_ar ? m.name_ar : m.name;
  }

  ministryName(user: UserDto): string {
    if (!user.ministryCode) return '—';
    const label = user.ministryName || user.ministryCode;
    return `${user.ministryCode} – ${label}`;
  }


  roleBadge(role: string): string {
    return role === 'ROLE_ADMIN' ? 'badge-admin' : 'badge-user';
  }

  roleLabel(role: string): string {
    return role === 'ROLE_ADMIN' ? 'Admin' : 'User';
  }

  initials(username: string): string {
    return username.slice(0, 2).toUpperCase();
  }
}
