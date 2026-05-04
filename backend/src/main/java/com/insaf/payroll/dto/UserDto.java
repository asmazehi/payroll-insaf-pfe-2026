package com.insaf.payroll.dto;

public class UserDto {
    private Long id;
    private String username;
    private String email;
    private String role;
    private String ministryCode;
    private boolean enabled;

    public UserDto() {}

    public UserDto(Long id, String username, String email, String role, String ministryCode, boolean enabled) {
        this.id = id;
        this.username = username;
        this.email = email;
        this.role = role;
        this.ministryCode = ministryCode;
        this.enabled = enabled;
    }

    public Long getId()              { return id; }
    public void setId(Long id)       { this.id = id; }
    public String getUsername()      { return username; }
    public void setUsername(String u){ this.username = u; }
    public String getEmail()         { return email; }
    public void setEmail(String e)   { this.email = e; }
    public String getRole()          { return role; }
    public void setRole(String r)    { this.role = r; }
    public String getMinistryCode()  { return ministryCode; }
    public void setMinistryCode(String m) { this.ministryCode = m; }
    public boolean isEnabled()       { return enabled; }
    public void setEnabled(boolean e){ this.enabled = e; }
}
