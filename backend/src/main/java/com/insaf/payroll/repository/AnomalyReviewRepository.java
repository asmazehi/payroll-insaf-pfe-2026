package com.insaf.payroll.repository;

import com.insaf.payroll.entity.AnomalyReview;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public interface AnomalyReviewRepository extends JpaRepository<AnomalyReview, Long> {

    Optional<AnomalyReview> findByEmployeeSkAndYearNumAndMonthNum(Long empSk, Integer year, Integer month);

    @Query("SELECT new map(r.status as status, COUNT(r) as count) FROM AnomalyReview r WHERE r.dismissedAt IS NULL GROUP BY r.status")
    List<Map<String, Object>> countByStatus();

    long countByStatus(String status);

    List<AnomalyReview> findByDismissedAtIsNotNull();

    @Modifying
    @Transactional
    @Query("DELETE FROM AnomalyReview r WHERE r.dismissedAt IS NOT NULL AND r.dismissedAt < :cutoff")
    void deleteByDismissedAtBefore(Instant cutoff);
}
