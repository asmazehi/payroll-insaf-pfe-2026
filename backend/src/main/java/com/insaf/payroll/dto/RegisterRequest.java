package com.insaf.payroll.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public class RegisterRequest {
    @NotBlank @Size(min = 3, max = 50)  private String username;
    @NotBlank @Email                     private String email;
    @NotBlank @Size(min = 6, max = 100) private String password;
    private String role = "ROLE_ANALYST";

    public String getUsername() { return username; }
    public void setUsername(String u) { this.username = u; }
    public String getEmail()    { return email; }
    public void setEmail(String e)    { this.email = e; }
    public String getPassword() { return password; }
    public void setPassword(String p) { this.password = p; }
    public String getRole()     { return role; }
    public void setRole(String r)     { this.role = r; }
}
