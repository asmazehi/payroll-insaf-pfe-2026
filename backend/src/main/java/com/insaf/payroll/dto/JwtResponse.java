package com.insaf.payroll.dto;

public class JwtResponse {
    private String token;
    private String type = "Bearer";
    private String username;
    private String role;
    private String ministryCode;

    public JwtResponse(String token, String username, String role, String ministryCode) {
        this.token       = token;
        this.username    = username;
        this.role        = role;
        this.ministryCode = ministryCode;
    }

    public String getToken()        { return token; }
    public String getType()         { return type; }
    public String getUsername()     { return username; }
    public String getRole()         { return role; }
    public String getMinistryCode() { return ministryCode; }
}
