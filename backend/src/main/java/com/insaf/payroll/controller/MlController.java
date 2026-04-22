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

    @PostMapping("/chat")
    public ResponseEntity<?> chat(@RequestBody Map<String, String> body) {
        String question = body.get("question");
        if (question == null || question.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "question is required"));
        }
        return ResponseEntity.ok(mlService.chat(question));
    }

    @GetMapping("/status")
    public ResponseEntity<?> status() {
        return ResponseEntity.ok(mlService.getMlStatus());
    }
}
