package com.insaf.payroll.dto;

public class JwtResponse {
    private String token;
    private String type = "Bearer";
    private String username;
    private String role;
    private String ministryCode;
    private boolean passwordChanged;

    public JwtResponse(String token, String username, String role,
                       String ministryCode, boolean passwordChanged) {
        this.token           = token;
        this.username        = username;
        this.role            = role;
        this.ministryCode    = ministryCode;
        this.passwordChanged = passwordChanged;
    }

    public String  getToken()           { return token; }
    public String  getType()            { return type; }
    public String  getUsername()        { return username; }
    public String  getRole()            { return role; }
    public String  getMinistryCode()    { return ministryCode; }
    public boolean isPasswordChanged()  { return passwordChanged; }
}
