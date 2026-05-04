package com.insaf.payroll.repository;

import com.insaf.payroll.entity.EtlJob;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface EtlJobRepository extends JpaRepository<EtlJob, Long> {
    List<EtlJob> findTop50ByOrderByStartedAtDesc();
    Optional<EtlJob> findByRunId(String runId);
}
