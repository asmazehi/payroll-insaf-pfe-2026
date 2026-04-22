import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { AuthGuard } from './guards/auth.guard';
import { LoginComponent }     from './pages/login/login.component';
import { DashboardComponent } from './pages/dashboard/dashboard.component';
import { ForecastComponent }  from './pages/forecast/forecast.component';
import { AnomaliesComponent } from './pages/anomalies/anomalies.component';
import { ChatbotComponent }   from './pages/chatbot/chatbot.component';

const routes: Routes = [
  { path: 'login',     component: LoginComponent },
  { path: 'dashboard', component: DashboardComponent, canActivate: [AuthGuard] },
  { path: 'forecast',  component: ForecastComponent,  canActivate: [AuthGuard] },
  { path: 'anomalies', component: AnomaliesComponent, canActivate: [AuthGuard] },
  { path: 'chatbot',   component: ChatbotComponent,   canActivate: [AuthGuard] },
  { path: '',          redirectTo: 'dashboard',        pathMatch: 'full' },
  { path: '**',        redirectTo: 'dashboard' },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {}
