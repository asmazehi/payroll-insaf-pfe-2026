import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { HttpClientModule, HTTP_INTERCEPTORS } from '@angular/common/http';
import { TranslateModule, TranslateLoader } from '@ngx-translate/core';
import { TranslateHttpLoader, provideTranslateHttpLoader } from '@ngx-translate/http-loader';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { MatToolbarModule }   from '@angular/material/toolbar';
import { MatSidenavModule }   from '@angular/material/sidenav';
import { MatListModule }      from '@angular/material/list';
import { MatCardModule }      from '@angular/material/card';
import { MatButtonModule }    from '@angular/material/button';
import { MatInputModule }     from '@angular/material/input';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatIconModule }      from '@angular/material/icon';
import { MatTableModule }     from '@angular/material/table';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSnackBarModule }  from '@angular/material/snack-bar';
import { MatSelectModule }    from '@angular/material/select';
import { MatTooltipModule }   from '@angular/material/tooltip';
import { MatTabsModule }      from '@angular/material/tabs';
import { MatChipsModule }     from '@angular/material/chips';
import { MatBadgeModule }     from '@angular/material/badge';
import { NgChartsModule }     from 'ng2-charts';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent }       from './app.component';
import { LoginComponent }     from './pages/login/login.component';
import { DashboardComponent } from './pages/dashboard/dashboard.component';
import { ForecastComponent }  from './pages/forecast/forecast.component';
import { AnomaliesComponent } from './pages/anomalies/anomalies.component';
import { ChatbotComponent }   from './pages/chatbot/chatbot.component';
import { ReportsComponent }   from './pages/reports/reports.component';
import { IngestComponent }    from './pages/ingest/ingest.component';
import { UsersComponent }     from './pages/users/users.component';
import { TicketsComponent }   from './pages/tickets/tickets.component';
import { ProfileComponent }   from './pages/profile/profile.component';
import { MonitoringComponent } from './pages/monitoring/monitoring.component';
import { NavbarComponent }    from './layout/navbar/navbar.component';
import { SidebarComponent }   from './layout/sidebar/sidebar.component';
import { JwtInterceptor }     from './interceptors/jwt.interceptor';

@NgModule({
  declarations: [
    AppComponent, LoginComponent, DashboardComponent,
    ForecastComponent, AnomaliesComponent, ChatbotComponent,
    ReportsComponent, IngestComponent, UsersComponent, TicketsComponent,
    ProfileComponent, MonitoringComponent, NavbarComponent, SidebarComponent
  ],
  imports: [
    BrowserModule, BrowserAnimationsModule,
    HttpClientModule, FormsModule, ReactiveFormsModule,
    AppRoutingModule, NgChartsModule,
    TranslateModule.forRoot({
      loader: { provide: TranslateLoader, useClass: TranslateHttpLoader },
      defaultLanguage: 'en'
    }),
    MatToolbarModule, MatSidenavModule, MatListModule, MatCardModule,
    MatButtonModule, MatInputModule, MatFormFieldModule, MatIconModule,
    MatTableModule, MatProgressSpinnerModule, MatSnackBarModule,
    MatSelectModule, MatTooltipModule, MatTabsModule, MatChipsModule, MatBadgeModule,
  ],
  providers: [
    { provide: HTTP_INTERCEPTORS, useClass: JwtInterceptor, multi: true },
    ...provideTranslateHttpLoader({ prefix: './assets/i18n/', suffix: '.json' })
  ],
  bootstrap: [AppComponent]
})
export class AppModule {}
