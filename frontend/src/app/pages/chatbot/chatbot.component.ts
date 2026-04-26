import { Component, OnInit, AfterViewChecked, ViewChild, ElementRef } from '@angular/core';
import { MlService } from '../../services/ml.service';

interface Message {
  id: string;
  role: 'user' | 'bot';
  text: string;
  timestamp: Date;
  isTyping?: boolean;
}

const STORAGE_KEY = 'insaf_chat_history';

const HINTS = [
  'How many employees are in the payroll?',
  'What is the total net pay for 2024?',
  'Which ministry has the highest payroll?',
  'Show me anomalies detected this year',
  'What is the average salary by grade?',
  'How has the payroll evolved over the years?',
];

@Component({
  selector: 'app-chatbot',
  templateUrl: './chatbot.component.html',
  styleUrls: ['./chatbot.component.scss']
})
export class ChatbotComponent implements OnInit, AfterViewChecked {
  @ViewChild('msgContainer') msgContainer!: ElementRef;

  messages: Message[] = [];
  input = '';
  loading = false;
  hints = HINTS;
  private shouldScroll = false;

  constructor(private ml: MlService) {}

  ngOnInit(): void {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        this.messages = parsed.map((m: any) => ({ ...m, timestamp: new Date(m.timestamp) }));
      } catch { this.messages = []; }
    }
  }

  ngAfterViewChecked(): void {
    if (this.shouldScroll) {
      this.scrollBottom();
      this.shouldScroll = false;
    }
  }

  get hasMessages(): boolean { return this.messages.length > 0; }

  send(text?: string): void {
    const q = (text || this.input).trim();
    if (!q || this.loading) return;

    this.messages.push({ id: this.uid(), role: 'user', text: q, timestamp: new Date() });
    this.input = '';
    this.loading = true;
    this.shouldScroll = true;

    const typingMsg: Message = { id: this.uid(), role: 'bot', text: '', timestamp: new Date(), isTyping: true };
    this.messages.push(typingMsg);

    this.ml.chat(q).subscribe({
      next: (res: any) => {
        const idx = this.messages.indexOf(typingMsg);
        const answer = res.answer || res.response || JSON.stringify(res);
        this.messages[idx] = { id: this.uid(), role: 'bot', text: answer, timestamp: new Date() };
        this.loading = false;
        this.shouldScroll = true;
        this.saveHistory();
      },
      error: () => {
        const idx = this.messages.indexOf(typingMsg);
        this.messages[idx] = {
          id: this.uid(), role: 'bot',
          text: 'Sorry, the AI service is currently unavailable. Please make sure the Python ML API is running.',
          timestamp: new Date()
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

  onKey(e: KeyboardEvent): void {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); this.send(); }
  }

  fmtTime(d: Date): string {
    return new Date(d).toLocaleTimeString('fr-TN', { hour: '2-digit', minute: '2-digit' });
  }

  private saveHistory(): void {
    const toSave = this.messages.filter(m => !m.isTyping);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(toSave));
  }

  private scrollBottom(): void {
    try {
      const el = this.msgContainer.nativeElement;
      el.scrollTop = el.scrollHeight;
    } catch {}
  }

  private uid(): string { return Math.random().toString(36).slice(2); }
}
