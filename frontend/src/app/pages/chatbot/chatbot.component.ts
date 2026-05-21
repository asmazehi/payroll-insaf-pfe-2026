import { Component, OnInit, OnDestroy, AfterViewChecked, ViewChild, ElementRef } from '@angular/core';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { MlService } from '../../services/ml.service';
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
}

const STORAGE_KEY = 'insaf_chat_history_v2';
const MAX_HISTORY  = 6;  // turns sent to LLM

const HINT_KEYS = ['h1','h2','h3','h4','h5','h6','h7','h8'];

@Component({
  selector: 'app-chatbot',
  templateUrl: './chatbot.component.html',
  styleUrls: ['./chatbot.component.scss']
})
export class ChatbotComponent implements OnInit, OnDestroy, AfterViewChecked {
  @ViewChild('msgContainer') msgContainer!: ElementRef;
  @ViewChild('inputRef') inputRef!: ElementRef;

  messages: Message[] = [];
  input = '';
  loading = false;
  hints: string[] = [];
  private shouldScroll = false;
  private langSub?: Subscription;

  constructor(
    private ml: MlService,
    private translate: TranslateService,
    private sanitizer: DomSanitizer,
  ) {}

  ngOnInit(): void {
    this.buildHints();
    this.langSub = this.translate.onLangChange.subscribe(() => this.buildHints());
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        this.messages = parsed.map((m: any) => ({
          ...m,
          timestamp: new Date(m.timestamp),
          html: m.role === 'bot' && m.text ? this.renderMarkdown(m.text) : undefined,
        }));
      } catch { this.messages = []; }
    }
  }

  ngOnDestroy(): void { this.langSub?.unsubscribe(); }

  ngAfterViewChecked(): void {
    if (this.shouldScroll) { this.scrollBottom(); this.shouldScroll = false; }
  }

  private buildHints(): void {
    this.hints = HINT_KEYS.map(k => this.translate.instant(`chatbot.hints.${k}`));
  }

  get hasMessages(): boolean { return this.messages.length > 0; }

  /** Convert last MAX_HISTORY non-typing messages to history array for the API */
  private buildHistory(): {role: string, text: string}[] {
    return this.messages
      .filter(m => !m.isTyping)
      .slice(-MAX_HISTORY)
      .map(m => ({ role: m.role === 'user' ? 'user' : 'bot', text: m.text }));
  }

  send(text?: string): void {
    const q = (text || this.input).trim();
    if (!q || this.loading) return;

    this.messages.push({ id: this.uid(), role: 'user', text: q, timestamp: new Date() });
    this.input = '';
    this.loading = true;
    this.shouldScroll = true;

    const history = this.buildHistory();
    const typingMsg: Message = { id: this.uid(), role: 'bot', text: '', timestamp: new Date(), isTyping: true };
    this.messages.push(typingMsg);

    const t0 = Date.now();
    this.ml.chat(q, history).subscribe({
      next: (res: any) => {
        const elapsed = Math.round((Date.now() - t0) / 100) / 10;
        const idx = this.messages.indexOf(typingMsg);
        const rawText = res.answer || res.response || JSON.stringify(res);
        this.messages[idx] = {
          id: this.uid(), role: 'bot',
          text: rawText,
          html: this.renderMarkdown(rawText),
          timestamp: new Date(),
          llm_used: res.llm_used,
          entities: res.entities,
          elapsed,
        };
        this.loading = false;
        this.shouldScroll = true;
        this.saveHistory();
      },
      error: () => {
        const idx = this.messages.indexOf(typingMsg);
        const errText = this.translate.instant('chatbot.ai_error');
        this.messages[idx] = {
          id: this.uid(), role: 'bot',
          text: errText,
          html: this.renderMarkdown(errText),
          timestamp: new Date(),
        };
        this.loading = false;
        this.shouldScroll = true;
        this.saveHistory();
      }
    });
  }

  clearHistory(): void {
    this.messages = [];
    localStorage.removeItem(STORAGE_KEY);
  }

  copyText(text: string): void {
    navigator.clipboard?.writeText(text).catch(() => {});
  }

  onKey(e: KeyboardEvent): void {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); this.send(); }
  }

  fmtTime(d: Date): string {
    return new Date(d).toLocaleTimeString('fr-TN', { hour: '2-digit', minute: '2-digit' });
  }

  /** Simple markdown → safe HTML renderer */
  renderMarkdown(text: string): SafeHtml {
    let html = text
      // Escape HTML entities first
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      // Bold **text**
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      // Italic *text*
      .replace(/\*([^*\n]+?)\*/g, '<em>$1</em>')
      // Inline code `code`
      .replace(/`([^`\n]+?)`/g, '<code>$1</code>')
      // Headers ### or ##
      .replace(/^###\s+(.+)$/gm, '<h4>$1</h4>')
      .replace(/^##\s+(.+)$/gm, '<h3>$1</h3>')
      // Horizontal rule
      .replace(/^---+$/gm, '<hr>')
      // Numbered lists
      .replace(/^(\d+)\.\s+(.+)$/gm, '<li class="ol-item"><span class="li-num">$1.</span> $2</li>')
      // Bullet lists (-, •, *)
      .replace(/^[-•]\s+(.+)$/gm, '<li class="ul-item">$1</li>')
      // Wrap consecutive <li> in <ul>
      .replace(/(<li[^>]*>.*?<\/li>\n?)+/gs, (match) => `<ul>${match}</ul>`)
      // Double newlines → paragraphs
      .replace(/\n\n+/g, '</p><p>')
      // Single newlines → <br>
      .replace(/\n/g, '<br>');

    html = `<p>${html}</p>`;
    // Clean up empty <p> tags
    html = html.replace(/<p>\s*<\/p>/g, '').replace(/<p>(<[hul])/g, '$1').replace(/(<\/[hul][^>]*>)<\/p>/g, '$1');

    return this.sanitizer.bypassSecurityTrustHtml(html);
  }

  hasEntities(m: Message): boolean {
    return m.entities && Object.keys(m.entities).length > 0;
  }

  formatEntities(e: any): string {
    const parts: string[] = [];
    if (e.years?.length)    parts.push(`Year: ${e.years.join(', ')}`);
    if (e.months?.length)   parts.push(`Month: ${e.months.join(', ')}`);
    if (e.ministry_code)    parts.push(`Ministry: ${e.ministry_code}`);
    if (e.grade_code)       parts.push(`Grade: ${e.grade_code}`);
    if (e.employee_sk)      parts.push(`Employee: ${e.employee_sk}`);
    if (e.top_n)            parts.push(`Top: ${e.top_n}`);
    return parts.join(' · ');
  }

  private saveHistory(): void {
    const toSave = this.messages
      .filter(m => !m.isTyping)
      .slice(-20)
      .map(({ html: _html, ...rest }) => rest); // don't persist SafeHtml
    localStorage.setItem(STORAGE_KEY, JSON.stringify(toSave));
  }

  private scrollBottom(): void {
    try { this.msgContainer.nativeElement.scrollTop = this.msgContainer.nativeElement.scrollHeight; } catch {}
  }

  private uid(): string { return Math.random().toString(36).slice(2); }
}
