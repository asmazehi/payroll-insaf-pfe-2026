import { Component, OnInit } from '@angular/core';
import { AdminService, UserDto } from '../../services/admin.service';

@Component({
  selector: 'app-users',
  templateUrl: './users.component.html',
  styleUrls: ['./users.component.scss']
})
export class UsersComponent implements OnInit {
  users: UserDto[] = [];
  loading = true;
  error = '';

  showForm = false;
  editingUser: UserDto | null = null;

  form = {
    username: '',
    email: '',
    password: '',
    role: 'ROLE_USER',
    ministryCode: ''
  };

  saving = false;
  saveError = '';

  constructor(private adminService: AdminService) {}

  ngOnInit(): void {
    this.load();
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
    this.form = { username: '', email: '', password: '', role: 'ROLE_USER', ministryCode: '' };
    this.saveError = '';
    this.showForm = true;
  }

  openEdit(user: UserDto): void {
    this.editingUser = user;
    this.form = {
      username: user.username,
      email: user.email,
      password: '',
      role: user.role,
      ministryCode: user.ministryCode ?? ''
    };
    this.saveError = '';
    this.showForm = true;
  }

  save(): void {
    this.saving = true;
    this.saveError = '';
    const payload: any = {
      username: this.form.username,
      email: this.form.email,
      role: this.form.role,
      ministryCode: this.form.ministryCode || null
    };
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

  roleBadge(role: string): string {
    return role === 'ROLE_ADMIN' ? 'badge-admin' : 'badge-user';
  }

  roleLabel(role: string): string {
    return role === 'ROLE_ADMIN' ? 'Admin' : 'User';
  }
}
