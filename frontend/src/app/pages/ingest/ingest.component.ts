import { Component, ElementRef, ViewChild } from '@angular/core';
import { HttpClient, HttpEventType, HttpRequest } from '@angular/common/http';

interface UploadResult {
  status: string;
  run_id: string;
  file: string;
  file_type: string;
  etl_stats: { records_processed?: number; records_loaded?: number; errors?: number; duration_s?: number; [key: string]: any };
  ml_status: string | null;
}

interface RunHistory {
  run_id: string;
  file: string;
  file_type: string;
  status: string;
  etl_stats: any;
  ml_status: string | null;
  ts: Date;
}

@Component({
  selector: 'app-ingest',
  templateUrl: './ingest.component.html',
  styleUrls: ['./ingest.component.scss']
})
export class IngestComponent {
  @ViewChild('fileInput') fileInputRef!: ElementRef<HTMLInputElement>;

  private readonly ML_API = 'http://localhost:8000';

  // Form state
  selectedFile: File | null = null;
  fileType: 'paie' | 'indem' = 'paie';
  retrain = false;
  dragging = false;

  // Upload state
  uploading = false;
  progress = 0;
  result: UploadResult | null = null;
  error = '';

  // History
  history: RunHistory[] = [];

  // ── Drag & drop ──────────────────────────────────────────────────

  onDragOver(e: DragEvent) { e.preventDefault(); this.dragging = true; }
  onDragLeave()            { this.dragging = false; }

  onDrop(e: DragEvent) {
    e.preventDefault();
    this.dragging = false;
    const f = e.dataTransfer?.files?.[0];
    if (f) this.setFile(f);
  }

  onFileChange(e: Event) {
    const f = (e.target as HTMLInputElement).files?.[0];
    if (f) this.setFile(f);
  }

  setFile(f: File) {
    if (!f.name.endsWith('.json') && !f.name.endsWith('.csv')) {
      this.error = 'Only JSON or CSV files are accepted.';
      return;
    }
    this.error = '';
    this.result = null;
    this.selectedFile = f;
  }

  removeFile() {
    this.selectedFile = null;
    this.result = null;
    this.error = '';
    if (this.fileInputRef) this.fileInputRef.nativeElement.value = '';
  }

  openPicker() { this.fileInputRef.nativeElement.click(); }

  // ── Upload ───────────────────────────────────────────────────────

  upload() {
    if (!this.selectedFile) return;
    this.uploading = true;
    this.progress  = 0;
    this.error     = '';
    this.result    = null;

    const body = new FormData();
    body.append('file', this.selectedFile, this.selectedFile.name);

    const req = new HttpRequest('POST',
      `${this.ML_API}/upload?file_type=${this.fileType}&retrain=${this.retrain}`,
      body, { reportProgress: true }
    );

    this.http.request(req).subscribe({
      next: (event: any) => {
        if (event.type === HttpEventType.UploadProgress && event.total) {
          this.progress = Math.round(100 * event.loaded / event.total);
        } else if (event.type === HttpEventType.Response) {
          this.result   = event.body as UploadResult;
          this.uploading = false;
          this.progress  = 100;
          this.history.unshift({ ...this.result!, ts: new Date() });
        }
      },
      error: (err) => {
        this.uploading = false;
        this.error = err?.error?.detail || err?.message || 'Upload failed. Make sure the Python API is running on port 8000.';
      }
    });
  }

  // ── Helpers ──────────────────────────────────────────────────────

  get fileSizeLabel(): string {
    if (!this.selectedFile) return '';
    const b = this.selectedFile.size;
    if (b < 1024)       return `${b} B`;
    if (b < 1024*1024)  return `${(b/1024).toFixed(1)} KB`;
    return `${(b/1024/1024).toFixed(2)} MB`;
  }

  statusClass(s: string): string {
    if (s === 'success') return 'badge-success';
    if (s === 'error')   return 'badge-danger';
    return 'badge-info';
  }

  fmt(n: number | undefined): string {
    if (n == null) return '—';
    return n.toLocaleString();
  }

  fmtTime(d: Date): string {
    return d.toLocaleTimeString('fr-TN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  getEtlKeys(stats: any): string[] {
    const skip = new Set(['records_processed', 'records_loaded', 'errors', 'duration_s']);
    return Object.keys(stats).filter(k => !skip.has(k) && stats[k] !== null && stats[k] !== undefined);
  }

  constructor(private http: HttpClient) {}
}
