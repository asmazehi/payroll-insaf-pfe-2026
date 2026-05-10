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

    private String ctFilter(String col, String ministryCode) {
        return hasFilter(ministryCode)
                ? "AND " + col + " = '" + ministryCode.replace("'", "''") + "'"
                : "";
    }

    // ── Summary ───────────────────────────────────────────────────────────────

    public Map<String, Object> getSummary(String ministryCode) {
        if (!hasFilter(ministryCode)) {
            // Aggregate the pre-computed monthly MV — sub-millisecond
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
        // Ministry-filtered: codetab index limits scan to that ministry only
        String f = ctFilter("fp.codetab", ministryCode);
        return jdbc.queryForMap("""
            SELECT
                COUNT(*)                        AS total_records,
                COUNT(DISTINCT fp.employee_sk)  AS total_employees,
                SUM(fp.m_netpay)                AS total_netpay,
                SUM(fp.m_salbrut)               AS total_grosspay,
                MIN(dt.year_num)                AS year_min,
                MAX(dt.year_num)                AS year_max
            FROM dw.fact_paie fp
            JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
            WHERE fp.employee_sk <> 0 AND dt.year_num > 0
            """ + f);
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
        String f = ctFilter("fp.codetab", ministryCode);
        return jdbc.queryForList("""
            SELECT
                dt.year_num,
                SUM(fp.m_netpay)                AS total_netpay,
                SUM(fp.m_salbrut)               AS total_grosspay,
                COUNT(DISTINCT fp.employee_sk)  AS employees,
                AVG(fp.m_netpay)                AS avg_netpay
            FROM dw.fact_paie fp
            JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
            WHERE fp.employee_sk <> 0 AND dt.year_num > 0
            """ + f + """
            GROUP BY dt.year_num
            ORDER BY dt.year_num
        """);
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
        String f = ctFilter("fp.codetab", ministryCode);
        return jdbc.queryForList("""
            SELECT
                dt.month_num,
                dt.month_start_date,
                SUM(fp.m_netpay)               AS total_netpay,
                SUM(fp.m_salbrut)              AS total_grosspay,
                COUNT(DISTINCT fp.employee_sk) AS employees
            FROM dw.fact_paie fp
            JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
            WHERE fp.employee_sk <> 0 AND dt.year_num = ?
            """ + f + """
            GROUP BY dt.month_num, dt.month_start_date
            ORDER BY dt.month_num
        """, year);
    }

    // ── Payroll by grade ──────────────────────────────────────────────────────

    public List<Map<String, Object>> getPayrollByGrade(String ministryCode) {
        if (!hasFilter(ministryCode)) {
            return jdbc.queryForList("""
                SELECT
                    grade_code,
                    grade_label_fr  AS lib_grade,
                    category,
                    employee_count  AS employees,
                    total_netpay,
                    avg_netpay
                FROM dw.mv_grade_distribution
                ORDER BY total_netpay DESC
                LIMIT 20
            """);
        }
        String f = ctFilter("fp.codetab", ministryCode);
        return jdbc.queryForList("""
            SELECT
                dg.grade_code,
                dg.grade_label_fr               AS lib_grade,
                dg.category,
                COUNT(DISTINCT fp.employee_sk)  AS employees,
                SUM(fp.m_netpay)               AS total_netpay,
                AVG(fp.m_netpay)               AS avg_netpay
            FROM dw.fact_paie fp
            JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
            WHERE fp.employee_sk <> 0 AND fp.grade_sk <> 0
            """ + f + """
            GROUP BY dg.grade_code, dg.grade_label_fr, dg.category
            ORDER BY total_netpay DESC
            LIMIT 20
        """);
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
        String f = ctFilter("fp.codetab", ministryCode);
        return jdbc.queryForList("""
            SELECT
                de2.libletabl                  AS ministry,
                fp.codetab,
                COUNT(DISTINCT fp.employee_sk)  AS employees,
                SUM(fp.m_netpay)               AS total_netpay,
                AVG(fp.m_netpay)               AS avg_netpay
            FROM dw.fact_paie fp
            JOIN dw.dim_etablissement de2 ON de2.codetab = fp.codetab
            WHERE fp.employee_sk <> 0 AND fp.codetab IS NOT NULL
            """ + f + """
            GROUP BY de2.libletabl, fp.codetab
            ORDER BY total_netpay DESC
            LIMIT 20
        """);
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
        String f = (ministryCode != null && !ministryCode.isBlank())
                ? "AND fi.codetab = '" + ministryCode.replace("'", "''") + "'"
                : "";
        return jdbc.queryForMap("""
            SELECT
                COUNT(*)                        AS total_records,
                COUNT(DISTINCT fi.employee_sk)  AS beneficiaries,
                SUM(fi.m_netpay)                AS total_amount,
                AVG(fi.m_netpay)                AS avg_amount
            FROM dw.fact_indem fi
            WHERE fi.employee_sk <> 0
            """ + f);
    }
}
