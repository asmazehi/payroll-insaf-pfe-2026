package com.insaf.payroll.controller;

import com.insaf.payroll.dto.CreateUserRequest;
import com.insaf.payroll.dto.UserDto;
import com.insaf.payroll.entity.User;
import com.insaf.payroll.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/admin/users")
@PreAuthorize("hasRole('ADMIN')")
public class UserController {

    @Autowired private UserRepository userRepository;
    @Autowired private PasswordEncoder passwordEncoder;

    @GetMapping
    public ResponseEntity<List<UserDto>> listUsers() {
        List<UserDto> users = userRepository.findAll().stream()
                .map(u -> new UserDto(u.getId(), u.getUsername(), u.getEmail(),
                        u.getRole(), u.getMinistryCode(), u.isEnabled()))
                .toList();
        return ResponseEntity.ok(users);
    }

    @PostMapping
    public ResponseEntity<?> createUser(@RequestBody CreateUserRequest req) {
        if (userRepository.existsByUsername(req.getUsername())) {
            return ResponseEntity.badRequest().body(Map.of("error", "Username already taken"));
        }
        if (userRepository.existsByEmail(req.getEmail())) {
            return ResponseEntity.badRequest().body(Map.of("error", "Email already registered"));
        }
        User user = new User();
        user.setUsername(req.getUsername());
        user.setEmail(req.getEmail());
        user.setPassword(passwordEncoder.encode(req.getPassword()));
        user.setRole(req.getRole() != null ? req.getRole() : "ROLE_USER");
        user.setMinistryCode(req.getMinistryCode());
        User saved = userRepository.save(user);
        return ResponseEntity.ok(new UserDto(saved.getId(), saved.getUsername(), saved.getEmail(),
                saved.getRole(), saved.getMinistryCode(), saved.isEnabled()));
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> updateUser(@PathVariable Long id, @RequestBody CreateUserRequest req) {
        return userRepository.findById(id).map(user -> {
            if (req.getEmail() != null) user.setEmail(req.getEmail());
            if (req.getRole() != null) user.setRole(req.getRole());
            if (req.getMinistryCode() != null) user.setMinistryCode(req.getMinistryCode());
            if (req.getPassword() != null && !req.getPassword().isBlank())
                user.setPassword(passwordEncoder.encode(req.getPassword()));
            User saved = userRepository.save(user);
            return ResponseEntity.ok(new UserDto(saved.getId(), saved.getUsername(), saved.getEmail(),
                    saved.getRole(), saved.getMinistryCode(), saved.isEnabled()));
        }).orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}/toggle")
    public ResponseEntity<?> toggleEnabled(@PathVariable Long id) {
        return userRepository.findById(id).map(user -> {
            user.setEnabled(!user.isEnabled());
            userRepository.save(user);
            return ResponseEntity.ok(Map.of("enabled", user.isEnabled()));
        }).orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteUser(@PathVariable Long id) {
        if (!userRepository.existsById(id)) return ResponseEntity.notFound().build();
        userRepository.deleteById(id);
        return ResponseEntity.ok(Map.of("message", "User deleted"));
    }
}
