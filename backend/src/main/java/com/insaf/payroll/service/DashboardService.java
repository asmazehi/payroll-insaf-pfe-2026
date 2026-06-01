package com.insaf.payroll.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class DashboardService {

    @Autowired private JdbcTemplate jdbc;

    private boolean hasFilter(String ministryCode) {
        return ministryCode != null && !ministryCode.isBlank();
    }

    // Sub-query that expands a top-level ministry code to all its establishments.
    // Uses v_ministry_codetabs view: includes the ministry itself + establishments
    // linked via codtutel + sport federations (natorg='8') under W00.
    private static final String SUBQ =
        "(SELECT sub_codetab FROM dw.v_ministry_codetabs WHERE ministry_codetab = ?)";

    // ── Summary ───────────────────────────────────────────────────────────────

    public Map<String, Object> getSummary(String ministryCode) {
        if (!hasFilter(ministryCode)) {
            return jdbc.queryForMap("""
                SELECT
                    SUM(employee_count)  AS total_records,
                    SUM(employee_count)  AS total_employees,
                    SUM(total_netpay)    AS total_netpay,
                    SUM(total_grosspay)  AS total_grosspay,
                    MIN(year_num)        AS year_min,
                    MAX(year_num)        AS year_max
                FROM dw.mv_payroll_by_month
            """);
        }
        return jdbc.queryForMap("""
            SELECT
                SUM(record_count)    AS total_records,
                SUM(employee_count)  AS total_employees,
                SUM(total_netpay)    AS total_netpay,
                SUM(total_grosspay)  AS total_grosspay,
                MIN(year_num)        AS year_min,
                MAX(year_num)        AS year_max
            FROM dw.mv_ministry_details
            WHERE codetab IN """ + SUBQ,
            ministryCode);
    }

    // ── Payroll by year ───────────────────────────────────────────────────────

    public List<Map<String, Object>> getPayrollByYear(String ministryCode) {
        if (!hasFilter(ministryCode)) {
            return jdbc.queryForList("""
                SELECT
                    year_num,
                    SUM(total_netpay)    AS total_netpay,
                    SUM(total_grosspay)  AS total_grosspay,
                    SUM(employee_count)  AS employees,
                    AVG(avg_netpay)      AS avg_netpay
                FROM dw.mv_payroll_by_month
                GROUP BY year_num
                ORDER BY year_num
            """);
        }
        return jdbc.queryForList("""
            SELECT
                year_num,
                SUM(total_netpay)    AS total_netpay,
                SUM(total_grosspay)  AS total_grosspay,
                SUM(employee_count)  AS employees,
                AVG(avg_netpay)      AS avg_netpay
            FROM dw.mv_ministry_details
            WHERE codetab IN """ + SUBQ + """
            GROUP BY year_num
            ORDER BY year_num
        """, ministryCode);
    }

    // ── Payroll by month ──────────────────────────────────────────────────────

    public List<Map<String, Object>> getPayrollByMonth(int year, String ministryCode) {
        if (!hasFilter(ministryCode)) {
            return jdbc.queryForList("""
                SELECT
                    month_num,
                    month_start_date,
                    total_netpay,
                    total_grosspay,
                    employee_count  AS employees,
                    avg_netpay
                FROM dw.mv_payroll_by_month
                WHERE year_num = ?
                ORDER BY month_num
            """, year);
        }
        return jdbc.queryForList("""
            SELECT
                month_num,
                MIN(month_start_date)  AS month_start_date,
                SUM(total_netpay)      AS total_netpay,
                SUM(total_grosspay)    AS total_grosspay,
                SUM(employee_count)    AS employees,
                AVG(avg_netpay)        AS avg_netpay
            FROM dw.mv_ministry_details
            WHERE codetab IN """ + SUBQ + """
              AND year_num = ?
            GROUP BY month_num
            ORDER BY month_num
        """, ministryCode, year);
    }

    // ── Payroll by grade ──────────────────────────────────────────────────────

    public List<Map<String, Object>> getPayrollByGrade(String ministryCode) {
        if (!hasFilter(ministryCode)) {
            return jdbc.queryForList("""
                SELECT grade_code, grade_label_fr AS lib_grade, category,
                       employee_count AS employees, total_netpay, avg_netpay
                FROM dw.mv_grade_distribution
                ORDER BY avg_netpay DESC
                LIMIT 8
            """);
        }
        return jdbc.queryForList("""
            SELECT
                mgm.grade_code,
                COALESCE(mgm.grade_label_fr, mgm.grade_code)  AS lib_grade,
                mgm.category,
                SUM(mgm.employee_count)  AS employees,
                SUM(mgm.total_netpay)    AS total_netpay,
                AVG(mgm.avg_netpay)      AS avg_netpay
            FROM dw.mv_grade_by_ministry mgm
            WHERE mgm.codetab IN """ + SUBQ + """
            GROUP BY mgm.grade_code, mgm.grade_label_fr, mgm.category
            ORDER BY avg_netpay DESC
            LIMIT 8
        """, ministryCode);
    }

    // ── Payroll by ministry ───────────────────────────────────────────────────

    public List<Map<String, Object>> getPayrollByMinistry(String ministryCode) {
        if (!hasFilter(ministryCode)) {
            return jdbc.queryForList("""
                SELECT
                    ministry_code    AS codetab,
                    ministry_name    AS ministry,
                    employee_count   AS employees,
                    total_netpay,
                    avg_netpay
                FROM dw.mv_payroll_by_ministry
                ORDER BY total_netpay DESC
                LIMIT 20
            """);
        }
        // For a ministry user, show breakdown by sub-establishment
        return jdbc.queryForList("""
            SELECT
                md.codetab,
                COALESCE(de.libletabl, de.libcetabl, md.codetab)  AS ministry,
                SUM(md.employee_count)                             AS employees,
                SUM(md.total_netpay)                               AS total_netpay,
                AVG(md.avg_netpay)                                 AS avg_netpay
            FROM dw.mv_ministry_details md
            LEFT JOIN dw.dim_etablissement de ON de.codetab = md.codetab
            WHERE md.codetab IN """ + SUBQ + """
            GROUP BY md.codetab, de.libletabl, de.libcetabl
            ORDER BY SUM(md.total_netpay) DESC
        """, ministryCode);
    }

    // ── Indemnity summary ─────────────────────────────────────────────────────

    public Map<String, Object> getIndemnitySummary(String ministryCode) {
        if (!hasFilter(ministryCode)) {
            return jdbc.queryForMap("""
                SELECT
                    SUM(employee_count)  AS total_records,
                    SUM(employee_count)  AS beneficiaries,
                    SUM(total_indemnity) AS total_amount,
                    AVG(avg_indemnity)   AS avg_amount
                FROM dw.mv_indem_by_month
            """);
        }
        // fact_indem uses organisme_sk — filter via dim_organisme.codetab
        // which maps the same 3-char codes as dim_etablissement.codetab
        return jdbc.queryForMap("""
            SELECT
                COUNT(*)                        AS total_records,
                COUNT(DISTINCT fi.employee_sk)  AS beneficiaries,
                SUM(fi.m_netpay)                AS total_amount,
                AVG(fi.m_netpay)                AS avg_amount
            FROM dw.fact_indem fi
            JOIN dw.dim_organisme dorg ON dorg.organisme_sk = fi.organisme_sk
            WHERE fi.employee_sk <> 0
              AND dorg.codetab IN """ + SUBQ,
            ministryCode);
    }
}
