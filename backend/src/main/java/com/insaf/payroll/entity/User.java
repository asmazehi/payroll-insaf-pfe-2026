package com.insaf.payroll.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "users", schema = "public")
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false, length = 50)
    private String username;

    @Column(nullable = false)
    private String password;

    @Column(unique = true, nullable = false, length = 100)
    private String email;

    @Column(length = 20)
    private String role = "ROLE_ANALYST";

    @Column(nullable = false)
    private boolean enabled = true;

    public Long getId()                  { return id; }
    public void setId(Long id)           { this.id = id; }

    public String getUsername()          { return username; }
    public void setUsername(String u)    { this.username = u; }

    public String getPassword()          { return password; }
    public void setPassword(String p)    { this.password = p; }

    public String getEmail()             { return email; }
    public void setEmail(String e)       { this.email = e; }

    public String getRole()              { return role; }
    public void setRole(String r)        { this.role = r; }

    public boolean isEnabled()           { return enabled; }
    public void setEnabled(boolean e)    { this.enabled = e; }
}
