import { Component, OnInit } from '@angular/core';
import { AdminService, UserDto, MinistryOption } from '../../services/admin.service';

@Component({
  selector: 'app-users',
  templateUrl: './users.component.html',
  styleUrls: ['./users.component.scss']
})
export class UsersComponent implements OnInit {
  users: UserDto[] = [];
  ministries: MinistryOption[] = [];
  loading = true;
  error = '';

  showForm = false;
  editingUser: UserDto | null = null;

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

  constructor(private adminService: AdminService) {}

  ngOnInit(): void {
    this.load();
    this.adminService.getMinistries().subscribe({
      next: (m) => this.ministries = m,
      error: () => {}
    });
  }

  load(): void {
    this.loading = true;
    this.adminService.getUsers().subscribe({
      next: (users) => { this.users = users; this.loading = false; },
      error: (err) => { this.error = err?.error?.error || 'Failed to load users'; this.loading = false; }
    });
  }

  openCreate(): void {
    this.editingUser = null;
    this.form = { username: '', email: '', password: '', role: 'ROLE_USER',
                  ministryCode: '', phone: '', profession: '', profilePhoto: '' };
    this.saveError = '';
    this.showForm = true;
  }

  openEdit(user: UserDto): void {
    this.editingUser = user;
    this.form = {
      username:     user.username,
      email:        user.email,
      password:     '',
      role:         user.role,
      ministryCode: user.ministryCode ?? '',
      phone:        user.phone ?? '',
      profession:   user.profession ?? '',
      profilePhoto: user.profilePhoto ?? ''
    };
    this.saveError = '';
    this.showForm = true;
  }

  onPhotoChange(event: Event): void {
    const file = (event.target as HTMLInputElement).files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => { this.form.profilePhoto = reader.result as string; };
    reader.readAsDataURL(file);
  }

  save(): void {
    this.saving = true;
    this.saveError = '';
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

    req.subscribe({
      next: () => { this.saving = false; this.showForm = false; this.load(); },
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
    if (!confirm(`Delete user "${user.username}"?`)) return;
    this.adminService.deleteUser(user.id).subscribe({
      next: () => this.load(),
      error: () => {}
    });
  }

  ministryName(code: string | null): string {
    if (!code) return '—';
    const m = this.ministries.find(x => x.code === code);
    return m ? `${code} – ${m.name}` : code;
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
