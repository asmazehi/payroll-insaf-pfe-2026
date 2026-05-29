package com.insaf.payroll.dto;

public class CreateUserRequest {
    private String username;
    private String email;
    private String password;
    private String role = "ROLE_USER";
    private String ministryCode;
    private String phone;
    private String profession;
    private String profilePhoto;

    public String getUsername()            { return username; }
    public void setUsername(String u)      { this.username = u; }
    public String getEmail()               { return email; }
    public void setEmail(String e)         { this.email = e; }
    public String getPassword()            { return password; }
    public void setPassword(String p)      { this.password = p; }
    public String getRole()                { return role; }
    public void setRole(String r)          { this.role = r; }
    public String getMinistryCode()        { return ministryCode; }
    public void setMinistryCode(String m)  { this.ministryCode = m; }
    public String getPhone()               { return phone; }
    public void setPhone(String p)         { this.phone = p; }
    public String getProfession()          { return profession; }
    public void setProfession(String p)    { this.profession = p; }
    public String getProfilePhoto()        { return profilePhoto; }
    public void setProfilePhoto(String p)  { this.profilePhoto = p; }
}
