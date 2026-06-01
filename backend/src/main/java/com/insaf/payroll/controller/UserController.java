package com.insaf.payroll.controller;

import com.insaf.payroll.dto.CreateUserRequest;
import com.insaf.payroll.dto.UserDto;
import com.insaf.payroll.entity.User;
import com.insaf.payroll.repository.UserRepository;
import com.insaf.payroll.service.EmailService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@RestController
@RequestMapping("/api/admin/users")
public class UserController {

    @Autowired private UserRepository userRepository;
    @Autowired private PasswordEncoder passwordEncoder;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private EmailService emailService;

    private boolean isAdmin(Authentication auth) {
        return auth.getAuthorities().stream()
                .anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN"));
    }

    // Build a codetab→name lookup once per request (avoids N+1 queries)
    private Map<String, String> buildNameLookup() {
        Map<String, String> lookup = new HashMap<>();
        jdbcTemplate.queryForList(
            "SELECT codetab, COALESCE(libletabl, libcetabl, codetab) AS name FROM dw.dim_etablissement"
        ).forEach(r -> lookup.put((String) r.get("codetab"), (String) r.get("name")));
        return lookup;
    }

    private UserDto toDto(User u, Map<String, String> nameLookup) {
        String code = u.getMinistryCode();
        String name = code != null ? nameLookup.getOrDefault(code, code) : null;
        return new UserDto(u.getId(), u.getUsername(), u.getEmail(), u.getRole(),
                code, name, u.getPhone(), u.getProfession(),
                u.getProfilePhoto(), u.isEnabled());
    }

    @GetMapping
    public ResponseEntity<List<UserDto>> listUsers(Authentication auth) {
        Map<String, String> nameLookup = buildNameLookup();
        Stream<User> stream = userRepository.findAll().stream();
        if (!isAdmin(auth)) {
            String myMinistry = userRepository.findByUsername(auth.getName())
                    .map(User::getMinistryCode).orElse(null);
            if (myMinistry != null) stream = stream.filter(u -> myMinistry.equals(u.getMinistryCode()));
            else stream = stream.filter(u -> false);
        }
        return ResponseEntity.ok(stream.map(u -> toDto(u, nameLookup)).toList());
    }

    @GetMapping("/ministries")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> listMinistries() {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
            "SELECT codetab AS code, " +
            "       COALESCE(libletabl, libcetabl, codetab) AS name, " +
            "       COALESCE(libletaba, libcetaba, libletabl, libcetabl, codetab) AS name_ar " +
            "FROM dw.dim_etablissement " +
            "WHERE natorg = '1' " +
            "  AND (codtutel IS NULL OR codtutel = codetab) " +
            "ORDER BY COALESCE(libletabl, libcetabl, codetab)");
        return ResponseEntity.ok(rows);
    }

    @GetMapping("/parent-ministry")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> getParentMinistry(@RequestParam String code) {
        // Given a sub-establishment code, return its parent ministry code.
        // If the code IS already a top-level ministry (natorg='1'), returns itself.
        // Used by the edit form to pre-populate the ministry→establishment cascade.
        try {
            String parent = jdbcTemplate.queryForObject(
                "SELECT ministry_codetab FROM dw.v_ministry_codetabs " +
                "WHERE sub_codetab = ? AND ministry_codetab != sub_codetab LIMIT 1",
                String.class, code);
            return ResponseEntity.ok(java.util.Map.of("parentMinistry", parent, "code", code));
        } catch (Exception e) {
            // Code is itself a top-level ministry (or unknown) — return it as its own parent
            return ResponseEntity.ok(java.util.Map.of("parentMinistry", code, "code", code));
        }
    }

    @GetMapping("/establishments")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> listEstablishments(@RequestParam String ministry) {
        // Returns all sub-establishments under the given ministry code.
        // Always includes the ministry itself as the first option ("— Entire ministry —").
        // Sub-establishments come from v_ministry_codetabs WHERE ministry_codetab = ?
        //   joined to dim_etablissement for their display name.
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
            "SELECT v.sub_codetab AS code, " +
            "       COALESCE(de.libletabl, de.libcetabl, v.sub_codetab) AS name, " +
            "       COALESCE(de.libletaba, de.libcetaba, de.libletabl, de.libcetabl, v.sub_codetab) AS name_ar " +
            "FROM dw.v_ministry_codetabs v " +
            "LEFT JOIN dw.dim_etablissement de ON de.codetab = v.sub_codetab " +
            "WHERE v.ministry_codetab = ? " +
            "ORDER BY CASE WHEN v.sub_codetab = ? THEN 0 ELSE 1 END, " +
            "         COALESCE(de.libletabl, de.libcetabl, v.sub_codetab)",
            ministry, ministry);
        return ResponseEntity.ok(rows);
    }

    @PostMapping
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> createUser(@RequestBody CreateUserRequest req) {
        if (userRepository.existsByUsername(req.getUsername()))
            return ResponseEntity.badRequest().body(Map.of("error", "Username already taken"));
        if (userRepository.existsByEmail(req.getEmail()))
            return ResponseEntity.badRequest().body(Map.of("error", "Email already registered"));

        User user = new User();
        user.setUsername(req.getUsername());
        user.setEmail(req.getEmail());
        user.setPassword(passwordEncoder.encode(req.getPassword()));
        user.setRole(req.getRole() != null ? req.getRole() : "ROLE_USER");
        user.setMinistryCode(req.getMinistryCode());
        user.setPhone(req.getPhone());
        user.setProfession(req.getProfession());
        user.setProfilePhoto(req.getProfilePhoto());
        Map<String, String> names = buildNameLookup();
        User saved = userRepository.save(user);

        // Send welcome email asynchronously — non-blocking, won't fail the request
        String ministryName = req.getMinistryCode() != null
            ? names.getOrDefault(req.getMinistryCode(), req.getMinistryCode()) : null;
        emailService.sendWelcomeEmail(saved.getEmail(), saved.getUsername(),
                                      req.getPassword(), ministryName);

        return ResponseEntity.ok(toDto(saved, names));
    }

    @PutMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> updateUser(@PathVariable Long id, @RequestBody CreateUserRequest req) {
        Map<String, String> names = buildNameLookup();
        return userRepository.findById(id).map(user -> {
            if (req.getEmail()       != null) user.setEmail(req.getEmail());
            if (req.getRole()        != null) user.setRole(req.getRole());
            user.setMinistryCode(req.getMinistryCode());
            if (req.getPhone()       != null) user.setPhone(req.getPhone());
            if (req.getProfession()  != null) user.setProfession(req.getProfession());
            if (req.getProfilePhoto()!= null) user.setProfilePhoto(req.getProfilePhoto());
            if (req.getPassword() != null && !req.getPassword().isBlank()) {
                user.setPassword(passwordEncoder.encode(req.getPassword()));
                user.setPasswordChanged(false); // force banner on next login
            }
            return ResponseEntity.ok(toDto(userRepository.save(user), names));
        }).orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}/toggle")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> toggleEnabled(@PathVariable Long id) {
        return userRepository.findById(id).map(user -> {
            user.setEnabled(!user.isEnabled());
            userRepository.save(user);
            return ResponseEntity.ok(Map.of("enabled", user.isEnabled()));
        }).orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> deleteUser(@PathVariable Long id) {
        if (!userRepository.existsById(id)) return ResponseEntity.notFound().build();
        userRepository.deleteById(id);
        return ResponseEntity.ok(Map.of("message", "User deleted"));
    }
}
