import { Component, ElementRef, NgZone, OnDestroy, ViewChild } from '@angular/core';
import { HttpClient, HttpEventType, HttpRequest } from '@angular/common/http';
import { environment } from '../../../environments/environment';

interface PipelineProgress {
  stage: string;
  pct: number;
  msg: string;
  ts: string;
  rows?: number;
  ml_status?: string;
  etl_stats?: any;
  dw_counts?: any;
  records_loaded?: any;
  quality_gate?: any;
}

interface UploadResult {
  status: string;
  run_id: string;
  file: string;
  file_type: string;
  etl_stats: any;
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

// Stage ordering for isStageDone() comparisons
const API_STAGE_ORDER = ['saved', 'etl_start', 'etl', 'quality_gate', 'dw_dims', 'dw_facts', 'ml', 'done'];

@Component({
  selector: 'app-ingest',
  templateUrl: './ingest.component.html',
  styleUrls: ['./ingest.component.scss']
})
export class IngestComponent implements OnDestroy {
  @ViewChild('fileInput') fileInputRef!: ElementRef<HTMLInputElement>;

  private readonly ML_API = environment.mlUrl;

  // Mode: 'upload' or 'path'
  mode: 'upload' | 'path' = 'path';

  // Upload mode state
  selectedFile: File | null = null;
  dragging = false;

  // Path mode state
  serverPath = '';
  pathDetected = '';

  // Shared options
  fileType: 'auto' | 'paie' | 'indem' = 'auto';
  retrain      = false;
  fullRetrain  = false;
  testMode  = false;
  testLimit = 50000;

  // Year filter (path mode only — avoids writing 40GB JSONL for the full file)
  yearFilter = false;
  yearMin: number | null = null;
  yearMax: number | null = null;

  // Pipeline state
  uploading = false;
  uploadPct  = 0;
  runId      = '';
  progress: PipelineProgress | null = null;
  result: UploadResult | null = null;
  error = '';

  // Elapsed timer
  elapsed = 0;
  private timer: any = null;
  private sse: EventSource | null = null;

  // History
  history: RunHistory[] = [];

  // Display stages for the tracker
  readonly displayStages = [
    { key: 'upload',       icon: 'upload',         label: 'Upload',       apiStages: ['saved'] },
    { key: 'etl',          icon: 'account_tree',   label: 'ETL',          apiStages: ['etl_start', 'etl'] },
    { key: 'quality_gate', icon: 'verified',       label: 'Quality Gate', apiStages: ['quality_gate'] },
    { key: 'dw',           icon: 'storage',        label: 'Load DW',      apiStages: ['dw_dims', 'dw_facts'] },
    { key: 'ml',           icon: 'model_training', label: 'ML Train',     apiStages: ['ml'] },
    { key: 'done',         icon: 'task_alt',       label: 'Done',         apiStages: ['done'] },
  ];

  constructor(private http: HttpClient, private ngZone: NgZone) {}

  ngOnDestroy() {
    this._stopSSE();
    this._stopTimer();
  }

  // ── Stage helpers ─────────────────────────────────────────────────

  get currentApiStage(): string { return this.progress?.stage || 'idle'; }

  isStageActive(stage: any): boolean {
    return stage.apiStages.includes(this.currentApiStage);
  }

  isStageDone(stage: any): boolean {
    if (this.currentApiStage === 'done') return true;
    const curIdx = API_STAGE_ORDER.indexOf(this.currentApiStage);
    if (curIdx < 0) return false;
    return stage.apiStages.every((s: string) => API_STAGE_ORDER.indexOf(s) < curIdx);
  }

  get currentPct(): number {
    if (this.uploadPct < 100) return Math.round(this.uploadPct * 0.08);
    return this.progress?.pct ?? 8;
  }

  get elapsedLabel(): string {
    const m = Math.floor(this.elapsed / 60);
    const s = this.elapsed % 60;
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
  }

  // ── Drag & drop (upload mode) ─────────────────────────────────────

  onDragOver(e: DragEvent) { e.preventDefault(); this.dragging = true; }
  onDragLeave()             { this.dragging = false; }

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
    const allowed = ['.json', '.jsonl', '.csv', '.xlsx', '.xls'];
    const ext = f.name.substring(f.name.lastIndexOf('.')).toLowerCase();
    if (!allowed.includes(ext)) {
      this.error = `Unsupported format. Accepted: ${allowed.join(', ')}`;
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

  // ── Run pipeline ──────────────────────────────────────────────────

  get canRun(): boolean {
    if (this.uploading) return false;
    return this.mode === 'path' ? !!this.serverPath.trim() : !!this.selectedFile;
  }

  run() {
    if (!this.canRun) return;
    this.uploading = true;
    this.uploadPct  = 0;
    this.progress   = null;
    this.error      = '';
    this.result     = null;
    this.runId      = '';
    this.elapsed    = 0;
    this._stopTimer();

    if (this.mode === 'path') {
      this._runFromPath();
    } else {
      this._runUpload();
    }
  }

  private _runFromPath() {
    const typeParam  = this.fileType !== 'auto' ? `&file_type=${this.fileType}` : '';
    const limitParam = this.testMode ? `&limit=${this.testLimit}` : '';
    const yearParams = (this.yearFilter && this.yearMin) ? `&year_min=${this.yearMin}${this.yearMax ? '&year_max=' + this.yearMax : ''}` : '';
    const fullRetainParam = this.fullRetrain ? `&full_retrain=true` : '';
    const url = `${this.ML_API}/ingest-path?file_path=${encodeURIComponent(this.serverPath.trim())}&retrain=${this.retrain}${typeParam}${limitParam}${yearParams}${fullRetainParam}`;

    this.http.post<any>(url, null).subscribe({
      next: (resp) => {
        this.runId    = resp.run_id;
        this.uploadPct = 100;
        this._startTimer();
        this._connectSSE(resp.run_id);
      },
      error: (err) => {
        this.uploading = false;
        this._stopTimer();
        this.error = err?.error?.detail || err?.message || 'Request failed. Make sure the Python API is running on port 8000.';
      }
    });
  }

  private _runUpload() {
    if (!this.selectedFile) return;
    const body = new FormData();
    body.append('file', this.selectedFile, this.selectedFile.name);

    const typeParam  = this.fileType !== 'auto' ? `&file_type=${this.fileType}` : '';
    const limitParam = this.testMode ? `&limit=${this.testLimit}` : '';
    const yearParams = (this.yearFilter && this.yearMin) ? `&year_min=${this.yearMin}${this.yearMax ? '&year_max=' + this.yearMax : ''}` : '';
    const req = new HttpRequest('POST',
      `${this.ML_API}/upload?retrain=${this.retrain}${typeParam}${limitParam}${yearParams}`,
      body, { reportProgress: true }
    );

    this.http.request(req).subscribe({
      next: (event: any) => {
        if (event.type === HttpEventType.UploadProgress && event.total) {
          this.uploadPct = Math.round(100 * event.loaded / event.total);
        } else if (event.type === HttpEventType.Response) {
          this.runId    = event.body.run_id;
          this.uploadPct = 100;
          this._startTimer();
          this._connectSSE(event.body.run_id);
        }
      },
      error: (err) => {
        this.uploading = false;
        this._stopTimer();
        this.error = err?.error?.detail || err?.message ||
          'Upload failed. For files >1 GB, use "Server File Path" mode instead.';
      }
    });
  }

  // ── SSE ───────────────────────────────────────────────────────────

  private _startTimer() {
    this.elapsed = 0;
    this.timer = setInterval(() => { this.elapsed++; }, 1000);
  }

  private _stopTimer() {
    if (this.timer) { clearInterval(this.timer); this.timer = null; }
  }

  private _stopSSE() {
    if (this.sse) { this.sse.close(); this.sse = null; }
  }

  private _connectSSE(runId: string) {
    this._stopSSE();
    this.sse = new EventSource(`${this.ML_API}/progress/${runId}`);

    this.sse.onmessage = (e: MessageEvent) => {
      this.ngZone.run(() => {
        try {
          const data: PipelineProgress = JSON.parse(e.data);
          this.progress = data;

          if (data.stage === 'done') {
            this._finishPipeline(runId, data, 'success');
          } else if (data.stage === 'error') {
            this.uploading = false;
            this._stopTimer();
            this._stopSSE();
            this.error = data.msg;
          }
        } catch { /* bad JSON frame — skip */ }
      });
    };

    this.sse.onerror = () => {
      this.ngZone.run(() => {
        if (this.uploading) {
          this.uploading = false;
          this._stopTimer();
          this._stopSSE();
          this.error = 'Connection to server lost. The pipeline may still be running — check the server logs.';
        }
      });
    };
  }

  private _finishPipeline(runId: string, data: PipelineProgress, status: string) {
    this.uploading = false;
    this._stopTimer();
    this._stopSSE();

    const filePaie  = data.records_loaded?.fact_paie;
    const fileIndem = data.records_loaded?.fact_indem;

    const r: UploadResult = {
      status,
      run_id:    runId,
      file:      data.etl_stats?.source_file || this.selectedFile?.name || this.serverPath.split(/[\\/]/).pop() || '',
      file_type: this.fileType === 'auto' ? (data.etl_stats?.pa_type === '3' ? 'indem' : 'paie') : this.fileType,
      etl_stats: {
        records_processed: data.etl_stats?.total_raw,
        records_loaded:    filePaie ?? fileIndem,
        errors:            data.etl_stats?.has_issues,
        duration_s:        this.elapsed,
        ...(data.etl_stats || {}),
      },
      ml_status: data.ml_status || null,
    };

    this.result = r;
    this.history.unshift({ ...r, ts: new Date() });
  }

  // ── Helpers ──────────────────────────────────────────────────────

  get fileSizeLabel(): string {
    if (!this.selectedFile) return '';
    const b = this.selectedFile.size;
    if (b < 1024)      return `${b} B`;
    if (b < 1048576)   return `${(b/1024).toFixed(1)} KB`;
    return `${(b/1048576).toFixed(2)} MB`;
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
    const skip = new Set(['records_processed', 'records_loaded', 'errors', 'duration_s',
                          'source_file', 'run_id', 'pa_type']);
    return Object.keys(stats).filter(k => !skip.has(k) && stats[k] !== null && stats[k] !== undefined);
  }
}
