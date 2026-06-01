package com.insaf.payroll.controller;

import com.insaf.payroll.entity.Ticket;
import com.insaf.payroll.repository.TicketRepository;
import com.insaf.payroll.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

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

    @DeleteMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> deleteTicket(@PathVariable Long id) {
        if (!ticketRepository.existsById(id)) return ResponseEntity.notFound().build();
        ticketRepository.deleteById(id);
        return ResponseEntity.ok(Map.of("message", "Ticket deleted"));
    }

    @GetMapping("/count/open")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> countOpen() {
        return ResponseEntity.ok(Map.of("count", ticketRepository.countByStatus("OPEN")));
    }
}
