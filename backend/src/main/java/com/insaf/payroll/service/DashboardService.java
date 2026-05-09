package com.insaf.payroll.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class DashboardService {

    @Autowired private JdbcTemplate jdbc;

    // Use codetab directly on fact_paie — avoids the expensive dim_organisme join
    private String codetabFilter(String ministryCode) {
        return (ministryCode != null && !ministryCode.isBlank())
                ? "AND fp.codetab = '" + ministryCode.replace("'", "''") + "'"
                : "";
    }

    public Map<String, Object> getSummary(String ministryCode) {
        String filter = codetabFilter(ministryCode);
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
            """ + filter);
    }

    public List<Map<String, Object>> getPayrollByYear(String ministryCode) {
        String filter = codetabFilter(ministryCode);
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
            """ + filter + """
            GROUP BY dt.year_num
            ORDER BY dt.year_num
        """);
    }

    public List<Map<String, Object>> getPayrollByMonth(int year, String ministryCode) {
        String filter = codetabFilter(ministryCode);
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
            """ + filter + """
            GROUP BY dt.month_num, dt.month_start_date
            ORDER BY dt.month_num
        """, year);
    }

    public List<Map<String, Object>> getPayrollByGrade(String ministryCode) {
        String filter = codetabFilter(ministryCode);
        return jdbc.queryForList("""
            SELECT
                dg.grade_code,
                dg.lib_grade,
                COUNT(DISTINCT fp.employee_sk)  AS employees,
                SUM(fp.m_netpay)               AS total_netpay,
                AVG(fp.m_netpay)               AS avg_netpay
            FROM dw.fact_paie fp
            JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
            WHERE fp.employee_sk <> 0 AND fp.grade_sk <> 0
            """ + filter + """
            GROUP BY dg.grade_code, dg.lib_grade
            ORDER BY total_netpay DESC
            LIMIT 20
        """);
    }

    public List<Map<String, Object>> getPayrollByMinistry(String ministryCode) {
        String filter = codetabFilter(ministryCode);
        return jdbc.queryForList("""
            SELECT
                de2.liborgl                    AS ministry,
                fp.codetab,
                COUNT(DISTINCT fp.employee_sk)  AS employees,
                SUM(fp.m_netpay)               AS total_netpay,
                AVG(fp.m_netpay)               AS avg_netpay
            FROM dw.fact_paie fp
            JOIN dw.dim_etablissement de2 ON de2.codetab = fp.codetab
            WHERE fp.employee_sk <> 0 AND fp.codetab IS NOT NULL
            """ + filter + """
            GROUP BY de2.liborgl, fp.codetab
            ORDER BY total_netpay DESC
            LIMIT 20
        """);
    }

    public Map<String, Object> getIndemnitySummary(String ministryCode) {
        String filter = (ministryCode != null && !ministryCode.isBlank())
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
            """ + filter);
    }
}
