package com.insaf.payroll.dto;

public class JwtResponse {
    private String token;
    private String type = "Bearer";
    private String username;
    private String role;

    public JwtResponse(String token, String username, String role) {
        this.token    = token;
        this.username = username;
        this.role     = role;
    }

    public String getToken()    { return token; }
    public String getType()     { return type; }
    public String getUsername() { return username; }
    public String getRole()     { return role; }
}
