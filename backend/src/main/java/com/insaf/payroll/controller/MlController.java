package com.insaf.payroll.controller;

import com.insaf.payroll.repository.UserRepository;
import com.insaf.payroll.service.MlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/ml")
public class MlController {

    @Autowired private MlService mlService;
    @Autowired private UserRepository userRepository;

    private String resolveMinistry(Authentication auth, String requested) {
        boolean isAdmin = auth.getAuthorities().stream()
                .anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN"));
        if (isAdmin) return requested;
        return userRepository.findByUsername(auth.getName())
                .map(u -> u.getMinistryCode())
                .orElse(null);
    }

    @GetMapping("/forecast")
    public ResponseEntity<?> forecast(@RequestParam(defaultValue = "6") int n) {
        return ResponseEntity.ok(mlService.getForecast(n));
    }

    @GetMapping("/anomalies")
    public ResponseEntity<?> anomalies(
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(required = false)    String ministry,
            @RequestParam(required = false)    Integer year,
            Authentication auth) {
        return ResponseEntity.ok(mlService.getAnomalies(limit, resolveMinistry(auth, ministry), year));
    }

    @GetMapping("/anomalies/by-ministry")
    public ResponseEntity<?> anomaliesByMinistry(Authentication auth) {
        String ministry = resolveMinistry(auth, null);
        return ResponseEntity.ok(mlService.getAnomaliesByMinistry(ministry));
    }

    @GetMapping("/anomalies/by-grade")
    public ResponseEntity<?> anomaliesByGrade(Authentication auth) {
        String ministry = resolveMinistry(auth, null);
        return ResponseEntity.ok(mlService.getAnomaliesByGrade(ministry));
    }

    @GetMapping("/anomalies/temporal-context")
    public ResponseEntity<?> anomalyTemporalContext(
            @RequestParam int employee_sk,
            @RequestParam int year_num,
            @RequestParam int month_num) {
        return ResponseEntity.ok(mlService.getAnomalyTemporalContext(employee_sk, year_num, month_num));
    }

    @PostMapping("/chat")
    public ResponseEntity<?> chat(@RequestBody Map<String, Object> body) {
        String question = (String) body.get("question");
        if (question == null || question.isBlank())
            return ResponseEntity.badRequest().body(Map.of("error", "question is required"));
        return ResponseEntity.ok(mlService.chat(body));
    }

    @GetMapping("/forecast/dimensions")
    public ResponseEntity<?> forecastDimensions(
            @RequestParam(required = false) String ministry,
            Authentication auth) {
        return ResponseEntity.ok(mlService.getForecastDimensions(resolveMinistry(auth, ministry)));
    }

    @GetMapping("/forecast/historical")
    public ResponseEntity<?> forecastHistorical(
            @RequestParam(required = false) String ministry,
            @RequestParam(required = false) String grade,
            Authentication auth) {
        return ResponseEntity.ok(mlService.getForecastHistorical(resolveMinistry(auth, ministry), grade));
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
