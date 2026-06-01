package com.insaf.payroll.controller;

import com.insaf.payroll.entity.User;
import com.insaf.payroll.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/profile")
public class ProfileController {

    @Autowired private UserRepository userRepository;
    @Autowired private PasswordEncoder passwordEncoder;

    @GetMapping
    public ResponseEntity<?> getProfile(Authentication auth) {
        return userRepository.findByUsername(auth.getName())
            .map(u -> ResponseEntity.ok(Map.of(
                "username",        u.getUsername(),
                "email",           u.getEmail(),
                "phone",           u.getPhone()           != null ? u.getPhone()           : "",
                "profession",      u.getProfession()      != null ? u.getProfession()      : "",
                "ministryCode",    u.getMinistryCode()    != null ? u.getMinistryCode()    : "",
                "role",            u.getRole(),
                "passwordChanged", u.isPasswordChanged()
            )))
            .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping
    public ResponseEntity<?> updateProfile(
            @RequestBody Map<String, String> body,
            Authentication auth) {

        return userRepository.findByUsername(auth.getName()).map(user -> {
            boolean changed = false;

            // Phone update
            String phone = body.get("phone");
            if (phone != null) {
                user.setPhone(phone.isBlank() ? null : phone.trim());
                changed = true;
            }

            // Password update — no current password required (admin set the initial one)
            String newPwd = body.get("newPassword");
            if (newPwd != null && !newPwd.isBlank()) {
                if (newPwd.length() < 6) {
                    return ResponseEntity.badRequest()
                        .body(Map.of("error", "New password must be at least 6 characters."));
                }
                user.setPassword(passwordEncoder.encode(newPwd));
                user.setPasswordChanged(true);
                changed = true;
            }

            if (!changed) {
                return ResponseEntity.badRequest().body(Map.of("error", "Nothing to update."));
            }

            userRepository.save(user);
            return ResponseEntity.ok(Map.of(
                "message",         "Profile updated successfully.",
                "passwordChanged", user.isPasswordChanged()
            ));
        }).orElse(ResponseEntity.notFound().build());
    }
}
