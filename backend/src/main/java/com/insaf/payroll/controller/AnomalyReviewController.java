package com.insaf.payroll.controller;

import com.insaf.payroll.entity.AnomalyReview;
import com.insaf.payroll.repository.AnomalyReviewRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/anomalies/reviews")
@PreAuthorize("hasRole('ADMIN')")
public class AnomalyReviewController {

    private static final Set<String> VALID = Set.of("LEGITIMATE", "ERROR", "INVESTIGATING");

    @Autowired private AnomalyReviewRepository repo;

    @GetMapping("/stats")
    public ResponseEntity<?> stats() {
        List<Map<String, Object>> counts = repo.countByStatus();
        long total = repo.count();
        return ResponseEntity.ok(Map.of("by_status", counts, "total_reviewed", total));
    }

    @PostMapping
    public ResponseEntity<?> review(@RequestBody Map<String, Object> body, Authentication auth) {
        String status = (String) body.get("status");
        if (!VALID.contains(status))
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid status. Use LEGITIMATE, ERROR or INVESTIGATING"));

        long empSk   = ((Number) body.get("employee_sk")).longValue();
        int  yearNum = ((Number) body.get("year_num")).intValue();
        int  monthNum= ((Number) body.get("month_num")).intValue();

        AnomalyReview review = repo
            .findByEmployeeSkAndYearNumAndMonthNum(empSk, yearNum, monthNum)
            .orElse(new AnomalyReview());

        review.setEmployeeSk(empSk);
        review.setYearNum(yearNum);
        review.setMonthNum(monthNum);
        review.setStatus(status);
        review.setNotes((String) body.get("notes"));
        review.setReviewedBy(auth.getName());

        return ResponseEntity.ok(repo.save(review));
    }

    @DeleteMapping
    public ResponseEntity<?> unmark(@RequestBody Map<String, Object> body) {
        long empSk    = ((Number) body.get("employee_sk")).longValue();
        int  yearNum  = ((Number) body.get("year_num")).intValue();
        int  monthNum = ((Number) body.get("month_num")).intValue();
        repo.findByEmployeeSkAndYearNumAndMonthNum(empSk, yearNum, monthNum)
            .ifPresent(repo::delete);
        return ResponseEntity.ok(Map.of("message", "Review removed"));
    }

    @GetMapping
    public ResponseEntity<List<AnomalyReview>> all() {
        return ResponseEntity.ok(repo.findAll());
    }
}
