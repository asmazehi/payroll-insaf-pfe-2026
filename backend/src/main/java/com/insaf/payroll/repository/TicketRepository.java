package com.insaf.payroll.repository;

import com.insaf.payroll.entity.Ticket;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface TicketRepository extends JpaRepository<Ticket, Long> {
    List<Ticket> findByCreatedByOrderByCreatedAtDesc(String createdBy);
    List<Ticket> findAllByOrderByCreatedAtDesc();
    long countByStatus(String status);
    List<Ticket> findByStatusAndResolvedAtBefore(String status, Instant cutoff);
    Optional<Ticket> findByIdAndCreatedBy(Long id, String createdBy);
}
