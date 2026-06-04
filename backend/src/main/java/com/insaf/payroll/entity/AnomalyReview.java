package com.insaf.payroll.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "anomaly_reviews", schema = "public",
       uniqueConstraints = @UniqueConstraint(columnNames = {"employee_sk","year_num","month_num"}))
public class AnomalyReview {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "employee_sk", nullable = false) private Long employeeSk;
    @Column(name = "year_num",    nullable = false) private Integer yearNum;
    @Column(name = "month_num",   nullable = false) private Integer monthNum;

    @Column(nullable = false, length = 20)
    private String status;   // LEGITIMATE | ERROR | INVESTIGATING

    @Column(columnDefinition = "TEXT")
    private String notes;

    @Column(name = "reviewed_by", length = 50) private String reviewedBy;
    @Column(name = "reviewed_at") private Instant reviewedAt = Instant.now();
    @Column(name = "dismissed_at") private Instant dismissedAt;

    public Long getId()          { return id; }
    public Long getEmployeeSk()  { return employeeSk; }
    public void setEmployeeSk(Long v) { this.employeeSk = v; }
    public Integer getYearNum()  { return yearNum; }
    public void setYearNum(Integer v) { this.yearNum = v; }
    public Integer getMonthNum() { return monthNum; }
    public void setMonthNum(Integer v) { this.monthNum = v; }
    public String getStatus()    { return status; }
    public void setStatus(String v) { this.status = v; this.reviewedAt = Instant.now(); }
    public String getNotes()     { return notes; }
    public void setNotes(String v) { this.notes = v; }
    public String getReviewedBy() { return reviewedBy; }
    public void setReviewedBy(String v) { this.reviewedBy = v; }
    public Instant getReviewedAt() { return reviewedAt; }
    public Instant getDismissedAt() { return dismissedAt; }
    public void setDismissedAt(Instant v) { this.dismissedAt = v; }
}
