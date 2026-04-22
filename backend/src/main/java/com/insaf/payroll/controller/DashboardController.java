package com.insaf.payroll.controller;

import com.insaf.payroll.service.DashboardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/dashboard")
public class DashboardController {

    @Autowired private DashboardService dashboardService;

    @GetMapping("/summary")
    public ResponseEntity<?> summary() {
        return ResponseEntity.ok(dashboardService.getSummary());
    }

    @GetMapping("/payroll-by-year")
    public ResponseEntity<?> payrollByYear() {
        return ResponseEntity.ok(dashboardService.getPayrollByYear());
    }

    @GetMapping("/payroll-by-month")
    public ResponseEntity<?> payrollByMonth(@RequestParam(defaultValue = "2025") int year) {
        return ResponseEntity.ok(dashboardService.getPayrollByMonth(year));
    }

    @GetMapping("/by-grade")
    public ResponseEntity<?> byGrade() {
        return ResponseEntity.ok(dashboardService.getPayrollByGrade());
    }

    @GetMapping("/by-ministry")
    public ResponseEntity<?> byMinistry() {
        return ResponseEntity.ok(dashboardService.getPayrollByMinistry());
    }

    @GetMapping("/indemnity-summary")
    public ResponseEntity<?> indemnitySummary() {
        return ResponseEntity.ok(dashboardService.getIndemnitySummary());
    }
}
