package com.insaf.payroll.controller;

import com.insaf.payroll.dto.JwtResponse;
import com.insaf.payroll.dto.LoginRequest;
import com.insaf.payroll.dto.RegisterRequest;
import com.insaf.payroll.entity.User;
import com.insaf.payroll.repository.UserRepository;
import com.insaf.payroll.security.JwtUtils;
import com.insaf.payroll.security.LoginRateLimiter;
import com.insaf.payroll.service.EmailService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.security.SecureRandom;
import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    @Autowired private AuthenticationManager authenticationManager;
    @Autowired private UserRepository userRepository;
    @Autowired private PasswordEncoder passwordEncoder;
    @Autowired private JwtUtils jwtUtils;
    @Autowired private LoginRateLimiter rateLimiter;
    @Autowired private EmailService emailService;

    private static final String CHARS = "ABCDEFGHJKMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789";
    private static final SecureRandom RNG = new SecureRandom();

    @PostMapping("/login")
    public ResponseEntity<?> login(@Valid @RequestBody LoginRequest req) {
        String username = req.getUsername();

        // Check lockout before attempting authentication
        if (rateLimiter.isLocked(username)) {
            long secs = rateLimiter.secondsRemaining(username);
            long mins  = (secs + 59) / 60;
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of(
                "error", "Account temporarily locked due to too many failed attempts. " +
                         "Try again in " + mins + " minute" + (mins == 1 ? "" : "s") + "."
            ));
        }

        try {
            Authentication auth = authenticationManager.authenticate(
                    new UsernamePasswordAuthenticationToken(username, req.getPassword())
            );
            SecurityContextHolder.getContext().setAuthentication(auth);
            rateLimiter.recordSuccess(username);   // reset counter on success

            UserDetails userDetails = (UserDetails) auth.getPrincipal();
            String role = userDetails.getAuthorities().iterator().next().getAuthority();

            User dbUser = userRepository.findByUsername(userDetails.getUsername()).orElse(null);
            String ministryCode = dbUser != null ? dbUser.getMinistryCode() : null;
            boolean pwdChanged  = dbUser != null && dbUser.isPasswordChanged();

            String token = jwtUtils.generateTokenWithMinistry(auth, ministryCode);
            return ResponseEntity.ok(new JwtResponse(token, userDetails.getUsername(), role, ministryCode, pwdChanged));

        } catch (BadCredentialsException ex) {
            rateLimiter.recordFailure(username);
            // Tell the user how many attempts remain before lockout
            boolean nowLocked = rateLimiter.isLocked(username);
            if (nowLocked) {
                return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of(
                    "error", "Too many failed attempts. Account locked for 15 minutes."
                ));
            }
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of(
                "error", "Invalid username or password."
            ));
        }
    }

    @PostMapping("/register")
    public ResponseEntity<?> register(@Valid @RequestBody RegisterRequest req) {
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
        user.setRole(req.getRole());
        userRepository.save(user);

        return ResponseEntity.ok(Map.of("message", "User registered successfully"));
    }

    @GetMapping("/me")
    public ResponseEntity<?> me(Authentication auth) {
        return ResponseEntity.ok(Map.of(
                "username", auth.getName(),
                "role", auth.getAuthorities().iterator().next().getAuthority()
        ));
    }

    @PostMapping("/forgot-password")
    public ResponseEntity<?> forgotPassword(@RequestBody Map<String, String> body) {
        String email = (body.getOrDefault("email", "")).trim();
        if (email.isBlank())
            return ResponseEntity.badRequest().body(Map.of("error", "Email is required"));

        // Always return the same response — never reveal whether the email exists
        userRepository.findByEmail(email).ifPresent(user -> {
            if ("ROLE_ADMIN".equals(user.getRole()) || !user.isEnabled()) return;

            // Generate a readable 10-char temp password (no ambiguous chars)
            StringBuilder sb = new StringBuilder(10);
            for (int i = 0; i < 10; i++) sb.append(CHARS.charAt(RNG.nextInt(CHARS.length())));
            String tempPwd = sb.toString();

            user.setPassword(passwordEncoder.encode(tempPwd));
            user.setPasswordChanged(false);
            userRepository.save(user);

            emailService.sendPasswordResetEmail(user.getEmail(), user.getUsername(), tempPwd, null);
        });

        return ResponseEntity.ok(Map.of(
            "message", "If this email is registered, a temporary password has been sent."
        ));
    }
}
