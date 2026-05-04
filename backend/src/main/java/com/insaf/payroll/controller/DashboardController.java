package com.insaf.payroll.controller;

import com.insaf.payroll.repository.UserRepository;
import com.insaf.payroll.service.DashboardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/dashboard")
public class DashboardController {

    @Autowired private DashboardService dashboardService;
    @Autowired private UserRepository userRepository;

    private String resolveMinistryCode(Authentication auth) {
        boolean isAdmin = auth.getAuthorities().stream()
                .anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN"));
        if (isAdmin) return null;
        return userRepository.findByUsername(auth.getName())
                .map(u -> u.getMinistryCode())
                .orElse(null);
    }

    @GetMapping("/summary")
    public ResponseEntity<?> summary(Authentication auth) {
        return ResponseEntity.ok(dashboardService.getSummary(resolveMinistryCode(auth)));
    }

    @GetMapping("/payroll-by-year")
    public ResponseEntity<?> payrollByYear(Authentication auth) {
        return ResponseEntity.ok(dashboardService.getPayrollByYear(resolveMinistryCode(auth)));
    }

    @GetMapping("/payroll-by-month")
    public ResponseEntity<?> payrollByMonth(@RequestParam(defaultValue = "2025") int year, Authentication auth) {
        return ResponseEntity.ok(dashboardService.getPayrollByMonth(year, resolveMinistryCode(auth)));
    }

    @GetMapping("/by-grade")
    public ResponseEntity<?> byGrade(Authentication auth) {
        return ResponseEntity.ok(dashboardService.getPayrollByGrade(resolveMinistryCode(auth)));
    }

    @GetMapping("/by-ministry")
    public ResponseEntity<?> byMinistry(Authentication auth) {
        return ResponseEntity.ok(dashboardService.getPayrollByMinistry(resolveMinistryCode(auth)));
    }

    @GetMapping("/indemnity-summary")
    public ResponseEntity<?> indemnitySummary(Authentication auth) {
        return ResponseEntity.ok(dashboardService.getIndemnitySummary(resolveMinistryCode(auth)));
    }
}
