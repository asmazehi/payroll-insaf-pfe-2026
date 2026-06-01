package com.insaf.payroll.controller;

import com.insaf.payroll.security.JwtUtils;
import com.insaf.payroll.service.MlService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import jakarta.servlet.http.HttpServletResponse;

import java.util.Map;

@RestController
@RequestMapping("/api/ml")
public class MlController {

    @Autowired private MlService mlService;
    @Autowired private JwtUtils jwtUtils;

    private boolean isAdmin(Authentication auth) {
        return auth.getAuthorities().stream()
                .anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN"));
    }

    private String ministryFromToken(HttpServletRequest request) {
        String header = request.getHeader("Authorization");
        if (StringUtils.hasText(header) && header.startsWith("Bearer ")) {
            return jwtUtils.getMinistryCodeFromToken(header.substring(7));
        }
        return null;
    }

    private static final String NO_MINISTRY = "__NONE__";

    private String resolveMinistry(Authentication auth, String requested, HttpServletRequest request) {
        if (isAdmin(auth)) return requested;
        String mc = ministryFromToken(request);
        return (mc != null && !mc.isBlank()) ? mc : NO_MINISTRY;
    }

    private boolean noMinistry(String mc) { return NO_MINISTRY.equals(mc); }

    @GetMapping("/forecast")
    public ResponseEntity<?> forecast(@RequestParam(defaultValue = "6") int n) {
        return ResponseEntity.ok(mlService.getForecast(n));
    }

    @GetMapping("/anomalies")
    public ResponseEntity<?> anomalies(
            @RequestParam(defaultValue = "50")  int limit,
            @RequestParam(required = false)     String ministry,
            @RequestParam(required = false)     Integer year,
            @RequestParam(defaultValue = "en")  String lang,
            Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistry(auth, ministry, request);
        if (noMinistry(mc)) return ResponseEntity.ok(java.util.Map.of(
            "anomalies", java.util.List.of(), "total_anomalies_in_report", 0,
            "unreviewed", 0, "review_stats", java.util.Map.of(),
            "severity_summary", java.util.Map.of("high",0,"medium",0,"low",0),
            "anomaly_rate_pct", 0.0, "has_new_cols", false, "returned", 0,
            "no_ministry", true));
        return ResponseEntity.ok(mlService.getAnomalies(limit, mc, year, lang));
    }

    @GetMapping("/anomalies/by-ministry")
    public ResponseEntity<?> anomaliesByMinistry(Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistry(auth, null, request);
        if (noMinistry(mc)) return ResponseEntity.ok(java.util.List.of());
        return ResponseEntity.ok(mlService.getAnomaliesByMinistry(mc));
    }

    @GetMapping("/anomalies/by-grade")
    public ResponseEntity<?> anomaliesByGrade(Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistry(auth, null, request);
        if (noMinistry(mc)) return ResponseEntity.ok(java.util.List.of());
        return ResponseEntity.ok(mlService.getAnomaliesByGrade(mc));
    }

    @GetMapping("/anomalies/temporal-context")
    public ResponseEntity<?> anomalyTemporalContext(
            @RequestParam int employee_sk,
            @RequestParam int year_num,
            @RequestParam int month_num) {
        return ResponseEntity.ok(mlService.getAnomalyTemporalContext(employee_sk, year_num, month_num));
    }

    @PostMapping("/chat")
    public ResponseEntity<?> chat(@RequestBody Map<String, Object> body,
                                  Authentication auth, HttpServletRequest request) {
        String question = (String) body.get("question");
        if (question == null || question.isBlank())
            return ResponseEntity.badRequest().body(Map.of("error", "question is required"));

        // Enforce ministry scope from JWT — never trust the client-provided ministry_code.
        // Admin  → null  (unrestricted, sees all ministries)
        // User   → their JWT ministry code (cannot be overridden by the client)
        String enforcedMinistry = isAdmin(auth) ? null : ministryFromToken(request);
        Map<String, Object> secureBody = new java.util.HashMap<>(body);
        secureBody.put("ministry_code", enforcedMinistry);

        return ResponseEntity.ok(mlService.chat(secureBody));
    }

    @PostMapping(value = "/chat/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public void chatStream(@RequestBody Map<String, Object> body,
                           Authentication auth, HttpServletRequest request,
                           HttpServletResponse response) throws Exception {
        String enforcedMinistry = isAdmin(auth) ? null : ministryFromToken(request);
        Map<String, Object> secureBody = new java.util.HashMap<>(body);
        secureBody.put("ministry_code", enforcedMinistry);

        response.setContentType(MediaType.TEXT_EVENT_STREAM_VALUE);
        response.setCharacterEncoding("UTF-8");
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("X-Accel-Buffering", "no");
        response.setHeader("Connection", "keep-alive");

        mlService.streamChat(secureBody, response.getOutputStream());
    }

    @GetMapping("/forecast/dimensions")
    public ResponseEntity<?> forecastDimensions(
            @RequestParam(required = false) String ministry,
            Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistry(auth, ministry, request);
        if (noMinistry(mc)) return ResponseEntity.ok(java.util.Map.of("ministries", java.util.List.of(), "grades", java.util.List.of()));
        return ResponseEntity.ok(mlService.getForecastDimensions(mc));
    }

    @GetMapping("/forecast/historical")
    public ResponseEntity<?> forecastHistorical(
            @RequestParam(required = false) String ministry,
            @RequestParam(required = false) String grade,
            Authentication auth, HttpServletRequest request) {
        String mc = resolveMinistry(auth, ministry, request);
        if (noMinistry(mc)) return ResponseEntity.ok(java.util.List.of());
        return ResponseEntity.ok(mlService.getForecastHistorical(mc, grade));
    }

    @GetMapping("/forecast/employee")
    public ResponseEntity<?> forecastEmployee(@RequestParam String employee_id) {
        return ResponseEntity.ok(mlService.getEmployeeForecast(employee_id));
    }

    @GetMapping("/status")
    public ResponseEntity<?> status() {
        return ResponseEntity.ok(mlService.getMlStatus());
    }
}
