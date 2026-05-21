package com.insaf.payroll.controller;

import com.insaf.payroll.service.MlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/ml")
public class MlController {

    @Autowired private MlService mlService;

    @GetMapping("/forecast")
    public ResponseEntity<?> forecast(@RequestParam(defaultValue = "6") int n) {
        return ResponseEntity.ok(mlService.getForecast(n));
    }

    @GetMapping("/anomalies")
    public ResponseEntity<?> anomalies(
            @RequestParam(defaultValue = "50")  int limit,
            @RequestParam(required = false)      String ministry,
            @RequestParam(required = false)      Integer year) {
        return ResponseEntity.ok(mlService.getAnomalies(limit, ministry, year));
    }

    @GetMapping("/anomalies/by-ministry")
    public ResponseEntity<?> anomaliesByMinistry() {
        return ResponseEntity.ok(mlService.getAnomaliesByMinistry());
    }

    @GetMapping("/anomalies/by-grade")
    public ResponseEntity<?> anomaliesByGrade() {
        return ResponseEntity.ok(mlService.getAnomaliesByGrade());
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
        if (question == null || question.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "question is required"));
        }
        return ResponseEntity.ok(mlService.chat(body));
    }

    @GetMapping("/forecast/dimensions")
    public ResponseEntity<?> forecastDimensions(
            @RequestParam(required = false) String ministry) {
        return ResponseEntity.ok(mlService.getForecastDimensions(ministry));
    }

    @GetMapping("/forecast/historical")
    public ResponseEntity<?> forecastHistorical(
            @RequestParam(required = false) String ministry,
            @RequestParam(required = false) String grade) {
        return ResponseEntity.ok(mlService.getForecastHistorical(ministry, grade));
    }

    @GetMapping("/forecast/employee")
    public ResponseEntity<?> forecastEmployee(
            @RequestParam String employee_id) {
        return ResponseEntity.ok(mlService.getEmployeeForecast(employee_id));
    }

    @GetMapping("/status")
    public ResponseEntity<?> status() {
        return ResponseEntity.ok(mlService.getMlStatus());
    }
}
