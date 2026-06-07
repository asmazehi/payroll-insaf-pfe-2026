import { Component, OnInit, OnDestroy, AfterViewChecked, ViewChild, ElementRef } from '@angular/core';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { MlService } from '../../services/ml.service';
import { AuthService } from '../../services/auth.service';
import { TranslateService } from '@ngx-translate/core';
import { Subscription } from 'rxjs';

interface Message {
  id: string;
  role: 'user' | 'bot';
  text: string;
  html?: SafeHtml;
  timestamp: Date;
  isTyping?: boolean;
  llm_used?: boolean;
  entities?: any;
  elapsed?: number;
  preview?: string;
  previewHtml?: SafeHtml;
  previewOpen?: boolean;
}

interface ChatSession {
  id: string;
  title: string;
  createdAt: string;
  messages: Omit<Message, 'html'>[];
}

const MAX_HISTORY  = 6;
const MAX_SESSIONS = 30;

// All possible autocomplete suggestions
const ALL_SUGGESTIONS = [
  'Quel est le salaire moyen par grade ?',
  'Montre-moi les tendances salariales',
  'Quels sont les grades les mieux payés ?',
  'Répartition des salaires par tranche',
  'Combien d\'employés dans ma direction ?',
  'Évolution de la masse salariale par année',
  'Quelles anomalies ont été détectées ?',
  'Prévisions pour les 6 prochains mois',
  'Comparaison des ministères par salaire',
  'Quel est le salaire moyen en 2025 ?',
  'Distribution des salaires cette année',
  'Quelles primes sont versées ?',
  'Analyse des déductions salariales',
  'Top 10 des grades par effectif',
  'Masse salariale du dernier mois',
  'Évolution du nombre de fonctionnaires',
  'Quels établissements ont le plus d\'employés ?',
  'Salaire médian par catégorie',
  'Anomalies récentes dans ma direction',
  'Répartition géographique des effectifs',
];

@Component({
  selector: 'app-chatbot',
  templateUrl: './chatbot.component.html',
  styleUrls: ['./chatbot.component.scss']
})
export class ChatbotComponent implements OnInit, OnDestroy, AfterViewChecked {
  @ViewChild('msgContainer') msgContainer!: ElementRef;
  @ViewChild('inputRef')     inputRef!:     ElementRef;

  messages: Message[]      = [];
  input                    = '';
  loading                  = false;
  sidebarOpen              = true;
  sessions: ChatSession[]  = [];
  activeSessionId          = '';
  suggestions: string[]    = [];
  showSuggestions          = false;

  private shouldScroll = false;
  private langSub?: Subscription;

  constructor(
    private ml:        MlService,
    private translate: TranslateService,
    private sanitizer: DomSanitizer,
    private auth:      AuthService,
  ) {}

  // ── Storage keys (per user) ──────────────────────────────────────────────────

  private get sessionsKey(): string {
    const u = this.auth.getCurrentUser();
    return u ? `insaf_sessions_${u.username}` : 'insaf_sessions';
  }

  private get activeKey(): string {
    const u = this.auth.getCurrentUser();
    return u ? `insaf_active_${u.username}` : 'insaf_active';
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────────

  ngOnInit(): void {
    this.langSub = this.translate.onLangChange.subscribe(() => {});
    this._loadSessions();
  }

  ngOnDestroy(): void {
    this.langSub?.unsubscribe();
  }

  ngAfterViewChecked(): void {
    if (this.shouldScroll) { this._scrollBottom(); this.shouldScroll = false; }
  }

  // ── Autocomplete ─────────────────────────────────────────────────────────────

  onInputChange(): void {
    const q = this.input.trim().toLowerCase();
    if (q.length < 2) { this.showSuggestions = false; this.suggestions = []; return; }

    this.suggestions = ALL_SUGGESTIONS
      .filter(s => s.toLowerCase().includes(q))
      .slice(0, 5);
    this.showSuggestions = this.suggestions.length > 0;
  }

  pickSuggestion(s: string): void {
    this.input = s;
    this.showSuggestions = false;
    this.send();
  }

  closeSuggestions(): void {
    setTimeout(() => { this.showSuggestions = false; }, 150);
  }

  // ── Session management ───────────────────────────────────────────────────────

  private _loadSessions(): void {
    try {
      const raw = localStorage.getItem(this.sessionsKey);
      this.sessions = raw ? JSON.parse(raw) : [];
    } catch { this.sessions = []; }

    const lastId = localStorage.getItem(this.activeKey);
    const found  = lastId ? this.sessions.find(s => s.id === lastId) : null;

    if (found) {
      this._activateSession(found.id, false);
    } else if (this.sessions.length > 0) {
      this._activateSession(this.sessions[0].id, false);
    } else {
      this._createSession();
    }
  }

  private _activateSession(id: string, save = true): void {
    this.activeSessionId = id;
    if (save) localStorage.setItem(this.activeKey, id);
    const session = this.sessions.find(s => s.id === id);
    this.messages = (session?.messages || []).map(m => ({
      ...m,
      timestamp: new Date(m.timestamp),
      html: m.role === 'bot' && m.text ? this.renderMarkdown(m.text) : undefined,
    }));
    this.shouldScroll = true;
  }

  private _createSession(): void {
    const session: ChatSession = {
      id: this.uid(), title: 'Nouvelle conversation',
      createdAt: new Date().toISOString(), messages: [],
    };
    this.sessions.unshift(session);
    this._saveSessions();
    this._activateSession(session.id);
  }

  newSession(): void  { this._createSession(); }
  loadSession(id: string): void { this._activateSession(id); }

  deleteSession(id: string, event: Event): void {
    event.stopPropagation();
    this.sessions = this.sessions.filter(s => s.id !== id);
    this._saveSessions();
    if (this.activeSessionId === id) {
      this.sessions.length > 0 ? this._activateSession(this.sessions[0].id) : this._createSession();
    }
  }

  clearCurrent(): void {
    const s = this.sessions.find(s => s.id === this.activeSessionId);
    if (s) { s.messages = []; s.title = 'Nouvelle conversation'; this._saveSessions(); }
    this.messages = [];
  }

  private _saveSessions(): void {
    if (this.sessions.length > MAX_SESSIONS) this.sessions = this.sessions.slice(0, MAX_SESSIONS);
    localStorage.setItem(this.sessionsKey, JSON.stringify(this.sessions));
  }

  private _saveCurrentMessages(): void {
    const session = this.sessions.find(s => s.id === this.activeSessionId);
    if (!session) return;
    session.messages = this.messages.filter(m => !m.isTyping).slice(-40)
      .map(({ html: _h, ...rest }) => rest);

    // Smart title: first user message, cleaned up
    if (session.title === 'Nouvelle conversation') {
      const first = session.messages.find(m => m.role === 'user');
      if (first) {
        let t = first.text.trim();
        t = t.charAt(0).toUpperCase() + t.slice(1);
        session.title = t.length > 45 ? t.slice(0, 42) + '…' : t;
      }
    }
    this._saveSessions();
  }

  // ── Messaging ────────────────────────────────────────────────────────────────

  get hasMessages(): boolean { return this.messages.filter(m => !m.isTyping).length > 0; }

  get hints(): string[] {
    return ALL_SUGGESTIONS.slice(0, 8);
  }

  private buildHistory(): {role: string, text: string}[] {
    return this.messages.filter(m => !m.isTyping).slice(-MAX_HISTORY)
      .map(m => ({ role: m.role === 'user' ? 'user' : 'bot', text: m.text }));
  }

  send(text?: string): void {
    const q = (text || this.input).trim();
    if (!q || this.loading) return;

    this.showSuggestions = false;
    this.messages.push({ id: this.uid(), role: 'user', text: q, timestamp: new Date() });
    this.input = '';
    this.loading = true;
    this.shouldScroll = true;

    const history    = this.buildHistory();
    const typingMsg: Message = { id: this.uid(), role: 'bot', text: '', timestamp: new Date(), isTyping: true };
    this.messages.push(typingMsg);

    const t0 = Date.now();
    let   botMsg: Message | null = null;

    this.ml.chatStream(q, history).subscribe({
      next: (chunk: any) => {
        // Create bot message bubble on first meaningful event (preview or token)
        if (!botMsg && (chunk.token || chunk.preview)) {
          botMsg = {
            id: this.uid(), role: 'bot', text: '',
            html: this.renderMarkdown(''), timestamp: new Date(),
            isTyping: false, previewOpen: false,
          };
          const idx = this.messages.indexOf(typingMsg);
          if (idx >= 0) this.messages[idx] = botMsg;
          this.loading = false;
        }

        // Data preview arrived before Ollama — show it immediately
        if (chunk.preview && botMsg) {
          botMsg.preview     = chunk.preview;
          botMsg.previewHtml = this.renderPreview(chunk.preview);
          botMsg.previewOpen = false;
          this.shouldScroll  = true;
        }

        if (chunk.token && botMsg) {
          botMsg.text += chunk.token;
          botMsg.html  = this.renderMarkdown(botMsg.text);
          this.shouldScroll = true;
        }

        if (chunk.done && botMsg) {
          botMsg.elapsed  = Math.round((Date.now() - t0) / 100) / 10;
          botMsg.entities = chunk.entities;
          botMsg.llm_used = true;
          this._saveCurrentMessages();
        }
      },
      error: () => {
        const idx     = this.messages.indexOf(typingMsg);
        const errText = '❌ Une erreur est survenue. Réessayez dans un moment.';
        this.messages[idx] = {
          id: this.uid(), role: 'bot', text: errText,
          html: this.renderMarkdown(errText), timestamp: new Date(),
        };
        this.loading = false;
        this.shouldScroll = true;
        this._saveCurrentMessages();
      }
    });
  }

  // ── Typewriter animation ─────────────────────────────────────────────────────


  // ── Formatting helpers ───────────────────────────────────────────────────────

  fmtTime(d: Date): string {
    return new Date(d).toLocaleTimeString('fr-TN', { hour: '2-digit', minute: '2-digit' });
  }

  fmtDate(iso: string): string {
    const d = new Date(iso);
    const diffDays = Math.floor((Date.now() - d.getTime()) / 86400000);
    if (diffDays === 0) return 'Aujourd\'hui';
    if (diffDays === 1) return 'Hier';
    if (diffDays < 7)  return `Il y a ${diffDays}j`;
    return d.toLocaleDateString('fr-TN', { day: 'numeric', month: 'short' });
  }

  copyText(text: string): void { navigator.clipboard?.writeText(text).catch(() => {}); }

  onKey(e: KeyboardEvent): void {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); this.send(); }
    if (e.key === 'Escape') { this.showSuggestions = false; }
    if (e.key === 'ArrowDown' && this.showSuggestions) { e.preventDefault(); }
  }

  hasEntities(m: Message): boolean {
    return m.entities && Object.keys(m.entities).filter(k => m.entities[k]).length > 0;
  }

  formatEntities(e: any): string {
    const p: string[] = [];
    if (e.years?.length)  p.push(e.years.join(', '));
    if (e.grade_code)     p.push(`Grade: ${e.grade_code}`);
    if (e.ministry_code)  p.push(`Min: ${e.ministry_code}`);
    return p.join(' · ');
  }

  togglePreview(m: Message): void { m.previewOpen = !m.previewOpen; }

  renderPreview(text: string): SafeHtml {
    const escaped = text
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    // Turn numbered rows into table rows
    const rows = escaped.split('\n').filter(l => l.trim());
    let html = '';
    for (const row of rows) {
      if (row.startsWith('  ') && row.match(/^\s+\d+\./)) {
        // Data row: "  1. key: val | key: val"
        const cells = row.replace(/^\s+\d+\.\s*/, '').split(' | ')
          .map(c => `<td>${c.trim()}</td>`).join('');
        html += `<tr>${cells}</tr>`;
      } else {
        html += `<div class="preview-title">${row.trim()}</div>`;
      }
    }
    const wrapped = html.includes('<tr>') ? `<table class="preview-table">${html}</table>` : html;
    return this.sanitizer.bypassSecurityTrustHtml(wrapped);
  }

  renderMarkdown(text: string): SafeHtml {
    let html = text
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*([^*\n]+?)\*/g, '<em>$1</em>')
      .replace(/`([^`\n]+?)`/g, '<code>$1</code>')
      .replace(/^###\s+(.+)$/gm, '<h4>$1</h4>')
      .replace(/^##\s+(.+)$/gm,  '<h3>$1</h3>')
      .replace(/^---+$/gm, '<hr>')
      .replace(/^(\d+)\.\s+(.+)$/gm, '<li class="ol-item"><span class="li-num">$1.</span> $2</li>')
      .replace(/^[-•]\s+(.+)$/gm, '<li class="ul-item">$1</li>')
      .replace(/(<li[^>]*>.*?<\/li>\n?)+/gs, m => `<ul>${m}</ul>`)
      .replace(/\n\n+/g, '</p><p>')
      .replace(/\n/g, '<br>');
    html = `<p>${html}</p>`;
    html = html.replace(/<p>\s*<\/p>/g, '').replace(/<p>(<[hul])/g, '$1').replace(/(<\/[hul][^>]*>)<\/p>/g, '$1');
    return this.sanitizer.bypassSecurityTrustHtml(html);
  }

  private _scrollBottom(): void {
    try { this.msgContainer.nativeElement.scrollTop = this.msgContainer.nativeElement.scrollHeight; } catch {}
  }

  private uid(): string { return Math.random().toString(36).slice(2); }
}
