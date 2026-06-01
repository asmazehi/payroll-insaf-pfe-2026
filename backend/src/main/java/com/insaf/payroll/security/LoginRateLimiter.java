package com.insaf.payroll.security;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory brute-force protection.
 * After MAX_ATTEMPTS failed logins within the window, the account is locked
 * for LOCKOUT_SECONDS. Entries auto-expire — no scheduled cleanup needed.
 */
@Component
public class LoginRateLimiter {

    private static final int  MAX_ATTEMPTS     = 5;
    private static final long LOCKOUT_SECONDS  = 15 * 60L;  // 15 minutes

    private record Entry(int attempts, Instant lockedUntil) {}

    private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();

    /** Returns true when the username is currently locked out. */
    public boolean isLocked(String username) {
        Entry e = map.get(username.toLowerCase());
        if (e == null) return false;
        if (e.lockedUntil() != null && Instant.now().isBefore(e.lockedUntil())) return true;
        if (e.lockedUntil() != null) map.remove(username.toLowerCase()); // lock expired
        return false;
    }

    /** How many seconds remain on the lockout (0 if not locked). */
    public long secondsRemaining(String username) {
        Entry e = map.get(username.toLowerCase());
        if (e == null || e.lockedUntil() == null) return 0;
        long rem = e.lockedUntil().getEpochSecond() - Instant.now().getEpochSecond();
        return Math.max(rem, 0);
    }

    /** Call on every failed login attempt. */
    public void recordFailure(String username) {
        map.compute(username.toLowerCase(), (k, e) -> {
            int attempts = (e == null ? 0 : e.attempts()) + 1;
            Instant lock = attempts >= MAX_ATTEMPTS
                ? Instant.now().plusSeconds(LOCKOUT_SECONDS)
                : (e != null ? e.lockedUntil() : null);
            return new Entry(attempts, lock);
        });
    }

    /** Call on successful login to reset the counter. */
    public void recordSuccess(String username) {
        map.remove(username.toLowerCase());
    }
}
