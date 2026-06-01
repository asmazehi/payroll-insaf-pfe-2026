package com.insaf.payroll.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/public")
public class PublicController {

    @Autowired private JdbcTemplate jdbc;

    private volatile Map<String, Object> cachedStats = null;
    private volatile long cacheTs = 0;
    private static final long TTL_MS = 3_600_000L; // 1 hour

    @GetMapping("/stats")
    public ResponseEntity<?> platformStats() {
        long now = System.currentTimeMillis();
        if (cachedStats != null && now - cacheTs < TTL_MS) {
            return ResponseEntity.ok(cachedStats);
        }
        try {
            Map<String, Object> row = jdbc.queryForMap("""
                SELECT
                    SUM(employee_count)   AS total_employees,
                    SUM(record_count)     AS total_records,
                    MIN(year_num)         AS year_min,
                    MAX(year_num)         AS year_max
                FROM dw.mv_ministry_details
            """);
            long emp  = row.get("total_employees") != null
                ? ((Number) row.get("total_employees")).longValue() : 0L;
            long rec  = row.get("total_records")   != null
                ? ((Number) row.get("total_records")).longValue()   : 0L;
            int  yMin = row.get("year_min") != null ? ((Number) row.get("year_min")).intValue() : 2010;
            int  yMax = row.get("year_max") != null ? ((Number) row.get("year_max")).intValue() : 2024;

            cachedStats = Map.of(
                "total_employees", emp,
                "total_records",   rec,
                "year_min",        yMin,
                "year_max",        yMax,
                "years_of_data",   yMax - yMin + 1
            );
            cacheTs = now;
            return ResponseEntity.ok(cachedStats);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of(
                "total_employees", 0,
                "total_records",   0,
                "year_min",        2010,
                "year_max",        2024,
                "years_of_data",   15
            ));
        }
    }
}
