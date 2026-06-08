package com.insaf.payroll.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "tickets", schema = "public")
public class Ticket {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 200)
    private String title;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(nullable = false, length = 20)
    private String status = "OPEN";

    @Column(name = "ministry_code", length = 10)
    private String ministryCode;

    @Column(name = "created_by", nullable = false, length = 50)
    private String createdBy;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    public Long getId()                    { return id; }
    public String getTitle()               { return title; }
    public void setTitle(String t)         { this.title = t; }
    public String getDescription()         { return description; }
    public void setDescription(String d)   { this.description = d; }
    public String getStatus()              { return status; }
    public String getMinistryCode()        { return ministryCode; }
    public void setMinistryCode(String m)  { this.ministryCode = m; }
    public String getCreatedBy()           { return createdBy; }
    public void setCreatedBy(String c)     { this.createdBy = c; }
    public Instant getCreatedAt()          { return createdAt; }
    public Instant getUpdatedAt()          { return updatedAt; }
    public Instant getResolvedAt()         { return resolvedAt; }
    public void setStatus(String s) {
        this.status = s;
        this.updatedAt = Instant.now();
        if ("DONE".equals(s) && this.resolvedAt == null) this.resolvedAt = Instant.now();
    }
}
