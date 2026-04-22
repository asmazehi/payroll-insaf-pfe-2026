import { Component } from '@angular/core';
import { MlService } from '../../services/ml.service';

interface Message { role: 'user' | 'bot'; text: string; }

@Component({
  selector: 'app-chatbot',
  templateUrl: './chatbot.component.html',
  styleUrls: ['./chatbot.component.scss']
})
export class ChatbotComponent {
  messages: Message[] = [
    { role: 'bot', text: 'Hello! Ask me anything about the INSAF payroll data.' }
  ];
  input   = '';
  loading = false;

  constructor(private ml: MlService) {}

  send(): void {
    const q = this.input.trim();
    if (!q || this.loading) return;
    this.messages.push({ role: 'user', text: q });
    this.input   = '';
    this.loading = true;
    this.ml.chat(q).subscribe({
      next: (res: any) => {
        this.messages.push({ role: 'bot', text: res.answer || res.response || JSON.stringify(res) });
        this.loading = false;
      },
      error: () => {
        this.messages.push({ role: 'bot', text: 'Error: ML service unavailable.' });
        this.loading = false;
      }
    });
  }

  onKey(e: KeyboardEvent): void {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); this.send(); }
  }
}
