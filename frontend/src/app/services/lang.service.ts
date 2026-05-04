import { Injectable } from '@angular/core';
import { TranslateService } from '@ngx-translate/core';

export type Lang = 'en' | 'fr' | 'ar';

const LANGS: { code: Lang; label: string; dir: 'ltr' | 'rtl' }[] = [
  { code: 'en', label: 'EN', dir: 'ltr' },
  { code: 'fr', label: 'FR', dir: 'ltr' },
  { code: 'ar', label: 'AR', dir: 'rtl' },
];

@Injectable({ providedIn: 'root' })
export class LangService {
  readonly langs = LANGS;
  private readonly STORAGE_KEY = 'insaf_lang';

  constructor(private translate: TranslateService) {
    translate.addLangs(['en', 'fr', 'ar']);
    translate.setDefaultLang('en');
    const saved = (localStorage.getItem(this.STORAGE_KEY) || 'en') as Lang;
    this.setLang(saved);
  }

  get current(): Lang {
    return this.translate.currentLang as Lang;
  }

  get isRtl(): boolean {
    return this.current === 'ar';
  }

  setLang(code: Lang): void {
    this.translate.use(code);
    localStorage.setItem(this.STORAGE_KEY, code);
    document.documentElement.lang = code;
    document.documentElement.dir = code === 'ar' ? 'rtl' : 'ltr';
    if (code === 'ar') {
      document.body.classList.add('rtl');
    } else {
      document.body.classList.remove('rtl');
    }
  }
}
