import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { AuthGuard } from './guards/auth.guard';
import { RoleGuard } from './guards/role.guard';
import { LoginComponent }    from './pages/login/login.component';
import { DashboardComponent } from './pages/dashboard/dashboard.component';
import { ForecastComponent }  from './pages/forecast/forecast.component';
import { AnomaliesComponent } from './pages/anomalies/anomalies.component';
import { ChatbotComponent }   from './pages/chatbot/chatbot.component';
import { ReportsComponent }   from './pages/reports/reports.component';
import { IngestComponent }    from './pages/ingest/ingest.component';
import { UsersComponent }     from './pages/users/users.component';
import { TicketsComponent }   from './pages/tickets/tickets.component';
import { ProfileComponent }    from './pages/profile/profile.component';
import { MonitoringComponent } from './pages/monitoring/monitoring.component';

const routes: Routes = [
  { path: 'login',     component: LoginComponent },
  { path: 'dashboard', component: DashboardComponent, canActivate: [AuthGuard] },
  { path: 'forecast',  component: ForecastComponent,  canActivate: [AuthGuard] },
  { path: 'anomalies', component: AnomaliesComponent, canActivate: [AuthGuard] },
  { path: 'reports',   component: ReportsComponent,   canActivate: [AuthGuard] },
  { path: 'chatbot',   component: ChatbotComponent,   canActivate: [AuthGuard] },
  { path: 'ingest',    component: IngestComponent,    canActivate: [AuthGuard, RoleGuard], data: { roles: ['ROLE_ADMIN'] } },
  { path: 'users',     component: UsersComponent,     canActivate: [AuthGuard, RoleGuard], data: { roles: ['ROLE_ADMIN'] } },
  { path: 'tickets',   component: TicketsComponent,   canActivate: [AuthGuard] },
  { path: 'profile',     component: ProfileComponent,    canActivate: [AuthGuard] },
  { path: 'monitoring',  component: MonitoringComponent, canActivate: [AuthGuard, RoleGuard], data: { roles: ['ROLE_ADMIN'] } },
  { path: '',          redirectTo: 'dashboard',        pathMatch: 'full' },
  { path: '**',        redirectTo: 'dashboard' },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {}
