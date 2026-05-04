package com.insaf.payroll.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "etl_jobs", schema = "public")
public class EtlJob {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "run_id", nullable = false, unique = true, length = 20)
    private String runId;

    @Column(name = "file_name", nullable = false)
    private String fileName;

    @Column(name = "file_type", nullable = false, length = 10)
    private String fileType;

    @Column(nullable = false, length = 20)
    private String status = "RUNNING";

    @Column(name = "started_at")
    private OffsetDateTime startedAt = OffsetDateTime.now();

    @Column(name = "finished_at")
    private OffsetDateTime finishedAt;

    @Column(name = "rows_written")
    private Integer rowsWritten;

    @Column(name = "qg_status", length = 40)
    private String qgStatus;

    @Column(name = "error_detail", columnDefinition = "TEXT")
    private String errorDetail;

    @Column(name = "uploaded_by", length = 50)
    private String uploadedBy;

    public Long getId()                        { return id; }
    public String getRunId()                   { return runId; }
    public void setRunId(String r)             { this.runId = r; }
    public String getFileName()                { return fileName; }
    public void setFileName(String f)          { this.fileName = f; }
    public String getFileType()                { return fileType; }
    public void setFileType(String t)          { this.fileType = t; }
    public String getStatus()                  { return status; }
    public void setStatus(String s)            { this.status = s; }
    public OffsetDateTime getStartedAt()       { return startedAt; }
    public void setStartedAt(OffsetDateTime d) { this.startedAt = d; }
    public OffsetDateTime getFinishedAt()      { return finishedAt; }
    public void setFinishedAt(OffsetDateTime d){ this.finishedAt = d; }
    public Integer getRowsWritten()            { return rowsWritten; }
    public void setRowsWritten(Integer r)      { this.rowsWritten = r; }
    public String getQgStatus()                { return qgStatus; }
    public void setQgStatus(String q)          { this.qgStatus = q; }
    public String getErrorDetail()             { return errorDetail; }
    public void setErrorDetail(String e)       { this.errorDetail = e; }
    public String getUploadedBy()              { return uploadedBy; }
    public void setUploadedBy(String u)        { this.uploadedBy = u; }
}
