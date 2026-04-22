import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { HttpClientModule, HTTP_INTERCEPTORS } from '@angular/common/http';
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
import { NgChartsModule }     from 'ng2-charts';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent }       from './app.component';
import { LoginComponent }     from './pages/login/login.component';
import { DashboardComponent } from './pages/dashboard/dashboard.component';
import { ForecastComponent }  from './pages/forecast/forecast.component';
import { AnomaliesComponent } from './pages/anomalies/anomalies.component';
import { ChatbotComponent }   from './pages/chatbot/chatbot.component';
import { NavbarComponent }    from './layout/navbar/navbar.component';
import { SidebarComponent }   from './layout/sidebar/sidebar.component';
import { JwtInterceptor }     from './interceptors/jwt.interceptor';

@NgModule({
  declarations: [
    AppComponent, LoginComponent, DashboardComponent,
    ForecastComponent, AnomaliesComponent, ChatbotComponent,
    NavbarComponent, SidebarComponent
  ],
  imports: [
    BrowserModule, BrowserAnimationsModule,
    HttpClientModule, FormsModule, ReactiveFormsModule,
    AppRoutingModule, NgChartsModule,
    MatToolbarModule, MatSidenavModule, MatListModule, MatCardModule,
    MatButtonModule, MatInputModule, MatFormFieldModule, MatIconModule,
    MatTableModule, MatProgressSpinnerModule, MatSnackBarModule,
  ],
  providers: [
    { provide: HTTP_INTERCEPTORS, useClass: JwtInterceptor, multi: true }
  ],
  bootstrap: [AppComponent]
})
export class AppModule {}
