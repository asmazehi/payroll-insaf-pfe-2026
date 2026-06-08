package com.insaf.payroll.controller;

import com.insaf.payroll.entity.Ticket;
import com.insaf.payroll.repository.TicketRepository;
import com.insaf.payroll.repository.UserRepository;
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

@RestController
@RequestMapping("/api/tickets")
public class TicketController {

    @Autowired private TicketRepository ticketRepository;
    @Autowired private UserRepository userRepository;

    private boolean isAdmin(Authentication auth) {
        return auth.getAuthorities().stream()
                .anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN"));
    }

    @PostMapping
    public ResponseEntity<?> createTicket(@RequestBody Map<String, String> body, Authentication auth) {
        String title = body.get("title");
        if (title == null || title.isBlank())
            return ResponseEntity.badRequest().body(Map.of("error", "Title is required"));

        Ticket t = new Ticket();
        t.setTitle(title.trim());
        t.setDescription(body.get("description"));
        t.setCreatedBy(auth.getName());

        userRepository.findByUsername(auth.getName())
                .ifPresent(u -> t.setMinistryCode(u.getMinistryCode()));

        return ResponseEntity.ok(ticketRepository.save(t));
    }

    @GetMapping
    public ResponseEntity<List<Ticket>> getTickets(Authentication auth) {
        if (isAdmin(auth)) {
            return ResponseEntity.ok(ticketRepository.findAllByOrderByCreatedAtDesc());
        }
        return ResponseEntity.ok(ticketRepository.findByCreatedByOrderByCreatedAtDesc(auth.getName()));
    }

    /** User can edit their own OPEN ticket (title/description only). */
    @PutMapping("/{id}")
    public ResponseEntity<?> editTicket(@PathVariable Long id,
                                        @RequestBody Map<String, String> body,
                                        Authentication auth) {
        if (isAdmin(auth)) return ResponseEntity.status(403).body(Map.of("error", "Admins use status endpoint"));

        return ticketRepository.findByIdAndCreatedBy(id, auth.getName())
                .map(t -> {
                    if (!"OPEN".equals(t.getStatus()))
                        return ResponseEntity.badRequest().<Object>body(Map.of("error", "Only OPEN tickets can be edited"));
                    String title = body.get("title");
                    if (title != null && !title.isBlank()) t.setTitle(title.trim());
                    if (body.containsKey("description"))    t.setDescription(body.get("description"));
                    return ResponseEntity.<Object>ok(ticketRepository.save(t));
                })
                .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}/status")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> updateStatus(@PathVariable Long id, @RequestBody Map<String, String> body) {
        String status = body.get("status");
        if (status == null) return ResponseEntity.badRequest().body(Map.of("error", "status required"));
        return ticketRepository.findById(id).map(t -> {
            t.setStatus(status.toUpperCase());
            return ResponseEntity.ok(ticketRepository.save(t));
        }).orElse(ResponseEntity.notFound().build());
    }

    /** Admin can delete any ticket; user can delete their own OPEN ticket. */
    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteTicket(@PathVariable Long id, Authentication auth) {
        if (isAdmin(auth)) {
            if (!ticketRepository.existsById(id)) return ResponseEntity.notFound().build();
            ticketRepository.deleteById(id);
            return ResponseEntity.ok(Map.of("message", "Ticket deleted"));
        }
        return ticketRepository.findByIdAndCreatedBy(id, auth.getName())
                .map(t -> {
                    if (!"OPEN".equals(t.getStatus()))
                        return ResponseEntity.badRequest().<Object>body(Map.of("error", "Only OPEN tickets can be deleted"));
                    ticketRepository.deleteById(id);
                    return ResponseEntity.<Object>ok(Map.of("message", "Ticket deleted"));
                })
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/count/open")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> countOpen() {
        return ResponseEntity.ok(Map.of("count", ticketRepository.countByStatus("OPEN")));
    }

    /** Auto-purge DONE tickets older than 5 days — runs every hour. */
    @Scheduled(fixedDelay = 3_600_000)
    public void purgeDoneTickets() {
        Instant cutoff = Instant.now().minus(5, ChronoUnit.DAYS);
        List<Ticket> old = ticketRepository.findByStatusAndResolvedAtBefore("DONE", cutoff);
        if (!old.isEmpty()) ticketRepository.deleteAll(old);
    }
}
