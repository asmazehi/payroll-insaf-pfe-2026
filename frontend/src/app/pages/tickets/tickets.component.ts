import { Component, OnInit } from '@angular/core';
import { AdminService, Ticket } from '../../services/admin.service';
import { AuthService } from '../../services/auth.service';

@Component({
  selector: 'app-tickets',
  templateUrl: './tickets.component.html',
  styleUrls: ['./tickets.component.scss']
})
export class TicketsComponent implements OnInit {
  tickets: Ticket[] = [];
  loading = true;
  error = '';

  showForm = false;
  form = { title: '', description: '' };
  saving = false;
  saveError = '';

  confirmDialog: { title: string; message: string; action: () => void } | null = null;

  constructor(private adminService: AdminService, public auth: AuthService) {}

  isAdmin(): boolean { return this.auth.isAdmin(); }

  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true;
    this.adminService.getTickets().subscribe({
      next: (t) => { this.tickets = t; this.loading = false; },
      error: () => { this.error = 'Failed to load tickets'; this.loading = false; }
    });
  }

  openCreate(): void {
    this.form = { title: '', description: '' };
    this.saveError = '';
    this.showForm = true;
  }

  submit(): void {
    if (!this.form.title.trim()) { this.saveError = 'Title is required'; return; }
    this.saving = true;
    this.saveError = '';
    this.adminService.createTicket(this.form.title, this.form.description).subscribe({
      next: () => { this.saving = false; this.showForm = false; this.load(); },
      error: (e) => { this.saving = false; this.saveError = e?.error?.error || 'Failed to submit ticket'; }
    });
  }

  setStatus(ticket: Ticket, status: string): void {
    this.adminService.updateTicketStatus(ticket.id, status).subscribe({
      next: (t) => { ticket.status = t.status; },
      error: () => {}
    });
  }

  remove(ticket: Ticket): void {
    this.confirmDialog = {
      title: 'Delete Ticket',
      message: `Remove <strong>"${ticket.title}"</strong> permanently?`,
      action: () => { this.adminService.deleteTicket(ticket.id).subscribe({ next: () => this.load() }); }
    };
  }

  confirmYes(): void { this.confirmDialog?.action(); this.confirmDialog = null; }
  confirmNo():  void { this.confirmDialog = null; }

  statusLabel(s: string): string {
    return { OPEN: 'Open', IN_PROGRESS: 'In Progress', DONE: 'Done' }[s] ?? s;
  }

  statusClass(s: string): string {
    return { OPEN: 'status-open', IN_PROGRESS: 'status-progress', DONE: 'status-done' }[s] ?? '';
  }
}
