import { Component, OnInit } from '@angular/core';
import { AdminService, Ticket } from '../../services/admin.service';
import { AuthService } from '../../services/auth.service';

const SEEN_KEY      = 'insaf_tickets_seen';
const DISMISSED_KEY = 'insaf_tickets_dismissed';

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

  editingTicket: Ticket | null = null;
  editForm = { title: '', description: '' };
  editSaving = false;
  editError = '';

  confirmDialog: { title: string; message: string; action: () => void } | null = null;

  resolvedNotifications: Ticket[] = [];

  constructor(private adminService: AdminService, public auth: AuthService) {}

  isAdmin(): boolean { return this.auth.isAdmin(); }

  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true;
    this.adminService.getTickets().subscribe({
      next: (tickets) => {
        this.tickets = tickets;
        this.loading = false;
        if (!this.isAdmin()) this.checkResolvedNotifications(tickets);
      },
      error: () => { this.error = 'Failed to load tickets'; this.loading = false; }
    });
  }

  /** Notify user about DONE tickets not yet explicitly dismissed. */
  private checkResolvedNotifications(tickets: Ticket[]): void {
    const raw = localStorage.getItem(DISMISSED_KEY);
    const dismissed: number[] = raw ? JSON.parse(raw) : [];

    this.resolvedNotifications = tickets.filter(t =>
      t.status === 'DONE' && !dismissed.includes(t.id)
    );
  }

  dismissNotification(ticket: Ticket): void {
    this.resolvedNotifications = this.resolvedNotifications.filter(t => t.id !== ticket.id);
    const raw = localStorage.getItem(DISMISSED_KEY);
    const dismissed: number[] = raw ? JSON.parse(raw) : [];
    if (!dismissed.includes(ticket.id)) dismissed.push(ticket.id);
    localStorage.setItem(DISMISSED_KEY, JSON.stringify(dismissed));
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

  openEdit(ticket: Ticket): void {
    this.editingTicket = ticket;
    this.editForm = { title: ticket.title, description: ticket.description ?? '' };
    this.editError = '';
  }

  submitEdit(): void {
    if (!this.editingTicket) return;
    if (!this.editForm.title.trim()) { this.editError = 'Title is required'; return; }
    this.editSaving = true;
    this.editError = '';
    this.adminService.editTicket(this.editingTicket.id, this.editForm.title, this.editForm.description).subscribe({
      next: (updated) => {
        this.editSaving = false;
        this.editingTicket = null;
        const idx = this.tickets.findIndex(t => t.id === updated.id);
        if (idx !== -1) this.tickets[idx] = updated;
      },
      error: (e) => { this.editSaving = false; this.editError = e?.error?.error || 'Failed to update ticket'; }
    });
  }

  setStatus(ticket: Ticket, status: string): void {
    this.adminService.updateTicketStatus(ticket.id, status).subscribe({
      next: (t) => { ticket.status = t.status; ticket.resolvedAt = t.resolvedAt; },
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

  canEdit(ticket: Ticket): boolean {
    return !this.isAdmin() && ticket.status === 'OPEN' &&
           ticket.createdBy === this.auth.getCurrentUser()?.username;
  }

  canDelete(ticket: Ticket): boolean {
    return !this.isAdmin() && ticket.status !== 'DONE' &&
           ticket.createdBy === this.auth.getCurrentUser()?.username;
  }
}
