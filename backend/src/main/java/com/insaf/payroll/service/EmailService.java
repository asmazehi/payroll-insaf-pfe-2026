package com.insaf.payroll.service;

import jakarta.mail.internet.MimeMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class EmailService {

    @Autowired private JavaMailSender mailSender;

    @Value("${app.mail.from}")      private String from;
    @Value("${app.mail.from-name}") private String fromName;

    @Async
    public void sendWelcomeEmail(String toEmail, String username, String temporaryPassword,
                                 String ministryName) {
        try {
            MimeMessage msg = mailSender.createMimeMessage();
            MimeMessageHelper h = new MimeMessageHelper(msg, true, "UTF-8");

            h.setFrom(from, fromName);
            h.setTo(toEmail);
            h.setSubject("Welcome to INSAF Payroll Platform – Your Account Details");

            String ministry = (ministryName != null && !ministryName.isBlank())
                ? "<p><strong>Ministry:</strong> " + ministryName + "</p>"
                : "";

            h.setText("""
                <!DOCTYPE html>
                <html>
                <body style="margin:0;padding:0;background:#0e0e1a;font-family:'Segoe UI',Arial,sans-serif;color:#e2e8f0;">
                  <div style="max-width:560px;margin:40px auto;background:#161625;border-radius:16px;overflow:hidden;border:1px solid rgba(255,255,255,0.08);">

                    <!-- Header -->
                    <div style="background:linear-gradient(135deg,#6366f1,#06b6d4);padding:32px 40px;">
                      <h1 style="margin:0;color:#fff;font-size:1.5rem;font-weight:700;letter-spacing:-0.01em;">INSAF</h1>
                      <p style="margin:4px 0 0;color:rgba(255,255,255,0.75);font-size:0.85rem;">Payroll Intelligence Platform</p>
                    </div>

                    <!-- Body -->
                    <div style="padding:36px 40px;">
                      <h2 style="margin:0 0 8px;color:#f8fafc;font-size:1.2rem;">Welcome, %s!</h2>
                      <p style="margin:0 0 24px;color:rgba(255,255,255,0.5);font-size:0.9rem;">
                        Your account has been created by an administrator. Here are your login details:
                      </p>

                      <div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.1);border-radius:12px;padding:20px 24px;margin-bottom:24px;">
                        <p style="margin:0 0 12px;"><strong style="color:rgba(255,255,255,0.4);font-size:0.75rem;text-transform:uppercase;letter-spacing:0.06em;">Username</strong><br>
                          <span style="color:#f8fafc;font-size:1rem;font-family:monospace;">%s</span></p>
                        <p style="margin:0 0 12px;"><strong style="color:rgba(255,255,255,0.4);font-size:0.75rem;text-transform:uppercase;letter-spacing:0.06em;">Temporary Password</strong><br>
                          <span style="color:#6ee7b7;font-size:1rem;font-family:monospace;">%s</span></p>
                        %s
                      </div>

                      <div style="background:rgba(251,191,36,0.08);border:1px solid rgba(251,191,36,0.25);border-radius:10px;padding:16px 20px;margin-bottom:28px;">
                        <p style="margin:0;color:#fbbf24;font-size:0.85rem;">
                          ⚠️ <strong>Please change your password</strong> after your first login for security purposes.
                        </p>
                      </div>

                      <p style="margin:0;color:rgba(255,255,255,0.35);font-size:0.8rem;line-height:1.6;">
                        If you did not expect this email, please contact your system administrator immediately.<br>
                        Do not share your credentials with anyone.
                      </p>
                    </div>

                    <!-- Footer -->
                    <div style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.06);">
                      <p style="margin:0;color:rgba(255,255,255,0.2);font-size:0.75rem;">
                        INSAF Payroll Intelligence Platform &middot; Automated message, do not reply
                      </p>
                    </div>
                  </div>
                </body>
                </html>
                """.formatted(username, username, temporaryPassword, ministry), true);

            mailSender.send(msg);
        } catch (Exception e) {
            System.err.println("[EmailService] Failed to send welcome email to " + toEmail + ": " + e.getMessage());
        }
    }

    @Async
    public void sendPasswordResetEmail(String toEmail, String username,
                                       String newPassword, String ministryName) {
        try {
            MimeMessage msg = mailSender.createMimeMessage();
            MimeMessageHelper h = new MimeMessageHelper(msg, true, "UTF-8");

            h.setFrom(from, fromName);
            h.setTo(toEmail);
            h.setSubject("INSAF – Your password has been reset");

            String ministry = (ministryName != null && !ministryName.isBlank())
                ? "<p><strong>Ministry:</strong> " + ministryName + "</p>" : "";

            h.setText("""
                <!DOCTYPE html>
                <html>
                <body style="margin:0;padding:0;background:#0e0e1a;font-family:'Segoe UI',Arial,sans-serif;color:#e2e8f0;">
                  <div style="max-width:560px;margin:40px auto;background:#161625;border-radius:16px;overflow:hidden;border:1px solid rgba(255,255,255,0.08);">
                    <div style="background:linear-gradient(135deg,#6366f1,#06b6d4);padding:32px 40px;">
                      <h1 style="margin:0;color:#fff;font-size:1.5rem;font-weight:700;">INSAF</h1>
                      <p style="margin:4px 0 0;color:rgba(255,255,255,0.75);font-size:0.85rem;">Payroll Intelligence Platform</p>
                    </div>
                    <div style="padding:36px 40px;">
                      <h2 style="margin:0 0 8px;color:#f8fafc;font-size:1.2rem;">Password Reset</h2>
                      <p style="margin:0 0 24px;color:rgba(255,255,255,0.5);font-size:0.9rem;">
                        Your password has been reset by an administrator. Use the credentials below to log in.
                      </p>
                      <div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.1);border-radius:12px;padding:20px 24px;margin-bottom:24px;">
                        <p style="margin:0 0 12px;"><strong style="color:rgba(255,255,255,0.4);font-size:0.75rem;text-transform:uppercase;letter-spacing:0.06em;">Username</strong><br>
                          <span style="color:#f8fafc;font-size:1rem;font-family:monospace;">%s</span></p>
                        <p style="margin:0 0 12px;"><strong style="color:rgba(255,255,255,0.4);font-size:0.75rem;text-transform:uppercase;letter-spacing:0.06em;">New Temporary Password</strong><br>
                          <span style="color:#6ee7b7;font-size:1rem;font-family:monospace;">%s</span></p>
                        %s
                      </div>
                      <div style="background:rgba(251,191,36,0.08);border:1px solid rgba(251,191,36,0.25);border-radius:10px;padding:16px 20px;">
                        <p style="margin:0;color:#fbbf24;font-size:0.85rem;">
                          ⚠️ <strong>Please change your password</strong> immediately after logging in.
                        </p>
                      </div>
                    </div>
                    <div style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.06);">
                      <p style="margin:0;color:rgba(255,255,255,0.2);font-size:0.75rem;">
                        INSAF Payroll Intelligence Platform &middot; Automated message, do not reply
                      </p>
                    </div>
                  </div>
                </body>
                </html>
                """.formatted(username, newPassword, ministry), true);

            mailSender.send(msg);
        } catch (Exception e) {
            System.err.println("[EmailService] Failed to send reset email to " + toEmail + ": " + e.getMessage());
        }
    }
}
