package com.insaf.payroll.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class DashboardService {

    @Autowired private JdbcTemplate jdbc;

    public Map<String, Object> getSummary() {
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
        """);
    }

    public List<Map<String, Object>> getPayrollByYear() {
        return jdbc.queryForList("""
            SELECT
                dt.year_num,
                SUM(fp.m_netpay)               AS total_netpay,
                SUM(fp.m_salbrut)              AS total_grosspay,
                COUNT(DISTINCT fp.employee_sk)  AS employees,
                AVG(fp.m_netpay)               AS avg_netpay
            FROM dw.fact_paie fp
            JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
            WHERE fp.employee_sk <> 0 AND dt.year_num > 0
            GROUP BY dt.year_num
            ORDER BY dt.year_num
        """);
    }

    public List<Map<String, Object>> getPayrollByMonth(int year) {
        return jdbc.queryForList("""
            SELECT
                dt.month_num,
                dt.month_start_date,
                SUM(fp.m_netpay)  AS total_netpay,
                SUM(fp.m_brut)    AS total_grosspay,
                COUNT(DISTINCT fp.employee_sk) AS employees
            FROM dw.fact_paie fp
            JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
            WHERE fp.employee_sk <> 0 AND dt.year_num = ?
            GROUP BY dt.month_num, dt.month_start_date
            ORDER BY dt.month_num
        """, year);
    }

    public List<Map<String, Object>> getPayrollByGrade() {
        return jdbc.queryForList("""
            SELECT
                dg.grade_code,
                dg.lib_grade,
                COUNT(DISTINCT fp.employee_sk)  AS employees,
                SUM(fp.m_netpay)               AS total_netpay,
                AVG(fp.m_netpay)               AS avg_netpay
            FROM dw.fact_paie fp
            JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
            WHERE fp.employee_sk <> 0
            GROUP BY dg.grade_code, dg.lib_grade
            ORDER BY total_netpay DESC
            LIMIT 20
        """);
    }

    public List<Map<String, Object>> getPayrollByMinistry() {
        return jdbc.queryForList("""
            SELECT
                do2.liborgl                    AS ministry,
                COUNT(DISTINCT fp.employee_sk)  AS employees,
                SUM(fp.m_netpay)               AS total_netpay,
                AVG(fp.m_netpay)               AS avg_netpay
            FROM dw.fact_paie fp
            JOIN dw.dim_organisme do2 ON do2.organisme_sk = fp.organisme_sk
            WHERE fp.employee_sk <> 0 AND fp.organisme_sk <> 0
            GROUP BY do2.liborgl
            ORDER BY total_netpay DESC
            LIMIT 20
        """);
    }

    public Map<String, Object> getIndemnitySummary() {
        return jdbc.queryForMap("""
            SELECT
                COUNT(*)                        AS total_records,
                COUNT(DISTINCT fi.employee_sk)  AS beneficiaries,
                SUM(fi.m_montant)               AS total_amount,
                AVG(fi.m_montant)               AS avg_amount
            FROM dw.fact_indem fi
            WHERE fi.employee_sk <> 0
        """);
    }
}
