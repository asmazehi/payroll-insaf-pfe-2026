package com.insaf.payroll.dto;

public class CreateUserRequest {
    private String username;
    private String email;
    private String password;
    private String role = "ROLE_USER";
    private String ministryCode;

    public String getUsername()      { return username; }
    public void setUsername(String u){ this.username = u; }
    public String getEmail()         { return email; }
    public void setEmail(String e)   { this.email = e; }
    public String getPassword()      { return password; }
    public void setPassword(String p){ this.password = p; }
    public String getRole()          { return role; }
    public void setRole(String r)    { this.role = r; }
    public String getMinistryCode()  { return ministryCode; }
    public void setMinistryCode(String m) { this.ministryCode = m; }
}
