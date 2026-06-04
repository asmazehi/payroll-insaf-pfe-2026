package com.insaf.payroll.controller;

import com.insaf.payroll.entity.AnomalyReview;
import com.insaf.payroll.repository.AnomalyReviewRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
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

    /** Examine an anomaly — sets a review status (LEGITIMATE / ERROR / INVESTIGATING). */
    @PostMapping
    public ResponseEntity<?> review(@RequestBody Map<String, Object> body, Authentication auth) {
        String status = (String) body.get("status");
        if (!VALID.contains(status))
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid status. Use LEGITIMATE, ERROR or INVESTIGATING"));

        long empSk    = ((Number) body.get("employee_sk")).longValue();
        int  yearNum  = ((Number) body.get("year_num")).intValue();
        int  monthNum = ((Number) body.get("month_num")).intValue();

        AnomalyReview review = repo
            .findByEmployeeSkAndYearNumAndMonthNum(empSk, yearNum, monthNum)
            .orElse(new AnomalyReview());

        review.setEmployeeSk(empSk);
        review.setYearNum(yearNum);
        review.setMonthNum(monthNum);
        review.setStatus(status);
        review.setNotes((String) body.get("notes"));
        review.setReviewedBy(auth.getName());
        review.setDismissedAt(null);  // un-dismiss if previously dismissed

        return ResponseEntity.ok(repo.save(review));
    }

    /** Dismiss an anomaly — hides it from the list for 10 days, then auto-purged. */
    @PostMapping("/dismiss")
    public ResponseEntity<?> dismiss(@RequestBody Map<String, Object> body, Authentication auth) {
        long empSk    = ((Number) body.get("employee_sk")).longValue();
        int  yearNum  = ((Number) body.get("year_num")).intValue();
        int  monthNum = ((Number) body.get("month_num")).intValue();

        AnomalyReview review = repo
            .findByEmployeeSkAndYearNumAndMonthNum(empSk, yearNum, monthNum)
            .orElse(new AnomalyReview());

        review.setEmployeeSk(empSk);
        review.setYearNum(yearNum);
        review.setMonthNum(monthNum);
        if (review.getStatus() == null) review.setStatus("LEGITIMATE");
        if (review.getReviewedBy() == null) review.setReviewedBy(auth.getName());
        review.setDismissedAt(Instant.now());

        return ResponseEntity.ok(repo.save(review));
    }

    /** Restore a dismissed anomaly — clears dismissed_at so it reappears in the list. */
    @PostMapping("/restore")
    public ResponseEntity<?> restore(@RequestBody Map<String, Object> body) {
        long empSk    = ((Number) body.get("employee_sk")).longValue();
        int  yearNum  = ((Number) body.get("year_num")).intValue();
        int  monthNum = ((Number) body.get("month_num")).intValue();

        repo.findByEmployeeSkAndYearNumAndMonthNum(empSk, yearNum, monthNum)
            .ifPresent(r -> { r.setDismissedAt(null); repo.save(r); });

        return ResponseEntity.ok(Map.of("message", "Restored"));
    }

    /** List all currently dismissed anomaly reviews (for the undo view). */
    @GetMapping("/dismissed")
    public ResponseEntity<List<AnomalyReview>> dismissed() {
        return ResponseEntity.ok(repo.findByDismissedAtIsNotNull());
    }

    /** Remove a review entirely (un-examine). */
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

    /** Purge dismissed anomalies older than 10 days — runs daily at 02:00. */
    @Scheduled(cron = "0 0 2 * * *")
    public void purgeOldDismissed() {
        repo.deleteByDismissedAtBefore(Instant.now().minus(10, ChronoUnit.DAYS));
    }
}
