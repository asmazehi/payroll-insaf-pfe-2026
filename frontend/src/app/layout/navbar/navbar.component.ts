import { Component } from '@angular/core';
import { AuthService } from '../../services/auth.service';

@Component({
  selector: 'app-navbar',
  templateUrl: './navbar.component.html',
  styleUrls: ['./navbar.component.scss']
})
export class NavbarComponent {
  user = this.auth.getCurrentUser();

  constructor(private auth: AuthService) {}

  logout(): void { this.auth.logout(); }
}
