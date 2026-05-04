package com.insaf.payroll.controller;

import com.insaf.payroll.entity.EtlJob;
import com.insaf.payroll.repository.EtlJobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/etl")
public class EtlJobController {

    @Autowired private EtlJobRepository etlJobRepository;

    @GetMapping("/jobs")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<List<EtlJob>> listJobs() {
        return ResponseEntity.ok(etlJobRepository.findTop50ByOrderByStartedAtDesc());
    }

    @GetMapping("/jobs/{runId}")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> getJob(@PathVariable String runId) {
        return etlJobRepository.findByRunId(runId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
