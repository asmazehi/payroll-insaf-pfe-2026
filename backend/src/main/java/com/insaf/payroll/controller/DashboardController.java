package com.insaf.payroll.controller;

import com.insaf.payroll.security.JwtUtils;
import com.insaf.payroll.service.DashboardService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dashboard")
public class DashboardController {

    @Autowired private DashboardService dashboardService;
    @Autowired private JwtUtils jwtUtils;

    private static final String NO_MINISTRY = "__NONE__";

    private String resolveMinistryCode(Authentication auth, HttpServletRequest request) {
        boolean isAdmin = auth.getAuthorities().stream()
                .anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN"));
        if (isAdmin) return null;   // null = no filter = all data (admin only)
        String header = request.getHeader("Authorization");
        if (StringUtils.hasText(header) && header.startsWith("Bearer ")) {
            String mc = jwtUtils.getMinistryCodeFromToken(header.substring(7));
            return (mc != null && !mc.isBlank()) ? mc : NO_MINISTRY;
        }
        return NO_MINISTRY;
    }

    private boolean noMinistry(String mc) { return NO_MINISTRY.equals(mc); }

    @GetMapping("/summary")
    public ResponseEntity<?> summary(Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistryCode(auth, request);
        if (noMinistry(mc)) return ResponseEntity.ok(Map.of(
            "total_records", 0, "total_employees", 0,
            "total_netpay", 0, "total_grosspay", 0,
            "year_min", 0, "year_max", 0,
            "no_ministry", true));
        return ResponseEntity.ok(dashboardService.getSummary(mc));
    }

    @GetMapping("/payroll-by-year")
    public ResponseEntity<?> payrollByYear(Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistryCode(auth, request);
        if (noMinistry(mc)) return ResponseEntity.ok(List.of());
        return ResponseEntity.ok(dashboardService.getPayrollByYear(mc));
    }

    @GetMapping("/payroll-by-month")
    public ResponseEntity<?> payrollByMonth(@RequestParam(defaultValue = "2025") int year,
                                             Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistryCode(auth, request);
        if (noMinistry(mc)) return ResponseEntity.ok(List.of());
        return ResponseEntity.ok(dashboardService.getPayrollByMonth(year, mc));
    }

    @GetMapping("/by-grade")
    public ResponseEntity<?> byGrade(Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistryCode(auth, request);
        if (noMinistry(mc)) return ResponseEntity.ok(List.of());
        return ResponseEntity.ok(dashboardService.getPayrollByGrade(mc));
    }

    @GetMapping("/by-ministry")
    public ResponseEntity<?> byMinistry(Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistryCode(auth, request);
        if (noMinistry(mc)) return ResponseEntity.ok(List.of());
        return ResponseEntity.ok(dashboardService.getPayrollByMinistry(mc));
    }

    @GetMapping("/indemnity-summary")
    public ResponseEntity<?> indemnitySummary(Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistryCode(auth, request);
        if (noMinistry(mc)) return ResponseEntity.ok(Map.of(
            "total_records", 0, "beneficiaries", 0, "total_amount", 0, "avg_amount", 0,
            "no_ministry", true));
        return ResponseEntity.ok(dashboardService.getIndemnitySummary(mc));
    }
}
