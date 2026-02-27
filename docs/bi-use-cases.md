# Business Intelligence — Payroll Use Cases

> **Project:** INSAF — PFE 2026
> **Datasets:** `schema-paie.json`, `schema-indemnity.json`, `grade.json`, `nature.json`, `region.json`, `organisme.json`, `indem_def.json`

---

## 1. Masse Salariale (Total Payroll Cost)

### 1.1 Dashboard — Vue globale de la masse salariale

| Indicator | Source Fields | Description |
|---|---|---|
| Masse salariale brute | `SUM(pa_salbrut)` | Total gross payroll cost across all employees |
| Masse salariale nette | `SUM(pa_netpay)` | Total net payroll disbursed |
| Total des retenues | `SUM(pa_cpe + pa_retrait + pa_cps + pa_capdeces + pa_sps + pa_spl)` | Total deductions (tax, pension, social) |
| Total des indemnités | `SUM(pa_sub)` filtered by `pa_type = 3` | Total indemnity/allowance spend |
| Coût par employé | `AVG(pa_salbrut)` | Average gross cost per employee |
| Taux de charges | `SUM(retenues) / SUM(pa_salbrut)` | Deduction rate as % of gross |

**Dimensions for slicing:**
- By ministry (`pa_codmin` → `organisme.LIBORGL`)
- By region (`pa_loca` → `region.LIB_REG`)
- By grade category (`pa_grd` → `grade.CAT`)
- By employment type (`pa_natu` → `nature.LIBNATL`)
- By pay period (`pa_mois`, `pa_annee`)

### 1.2 Évolution mensuelle et annuelle

- **Monthly trend:** `pa_salbrut` aggregated by `pa_mois` + `pa_annee` — track seasonality (bonuses in specific months, Ramadan allowances, etc.)
- **Year-over-year growth:** compare `SUM(pa_salbrut)` across `pa_annee` values
- **Indemnity vs. base salary ratio:** `SUM(pa_sub) / SUM(pa_salbrut)` over time — is the allowance share growing?

### 1.3 Répartition structurelle

| Analysis | Dimensions | Business Question |
|---|---|---|
| By ministry | `pa_codmin` | Which ministries have the highest payroll cost? |
| By grade | `pa_grd` → `grade.CAT` | What is the cost distribution across grade categories? |
| By nature | `pa_natu` → `nature.LIBNATL` | Cost of permanent vs. contractual vs. temporary workers? |
| By region | `pa_loca` → `region.LIB_REG` | Geographic payroll distribution — which regions cost the most? |
| By directorate | `pa_dire` → `organisme.LIBORGL` | Payroll cost per organizational unit? |

---

## 2. Prédiction — Forecasting Future Payroll

### 2.1 Projection de la masse salariale

| Model | Input Features | Target | Use Case |
|---|---|---|---|
| Time series (ARIMA / Prophet) | Historical `pa_salbrut` by month | Future months' total payroll | Budget planning for next fiscal year |
| Linear regression | `pa_annee`, headcount, `AVG(pa_eche)` | Annual payroll cost | Multi-year budget forecast |
| Grade-based projection | `pa_grd`, `pa_eche`, `grade.NIVDEP` | Expected salary per employee | Simulate echelon advancement impact |

### 2.2 Projection des effectifs et départs en retraite

Using `pa_datnais` + `grade.AGERET`:

- **Retirement wave forecast:** calculate expected retirement year per employee → predict headcount drop by year
- **Replacement cost modeling:** retiring employees' grade × new recruit starting echelon (`grade.NIVDEP`) → net cost change
- **Age pyramid:** distribution of employees by age bracket per ministry — identify aging workforce risk

### 2.3 Simulation d'impact

| Scenario | How to Model |
|---|---|
| General salary increase (+X%) | Apply multiplier to `pa_salbrut` → new total masse salariale |
| Echelon advancement | Increment `pa_eche` for eligible employees → recalculate using salary grid |
| New recruitment batch | Add N employees at `grade.NIVDEP` echelon → project additional cost |
| Indemnity policy change | Modify `indem_def` rates → recalculate `pa_sub` totals |
| Grade reclassification | Move employees between grades → compare before/after cost |

---

## 3. Statistiques descriptives

### 3.1 Profil des effectifs

| Statistic | Fields | Output |
|---|---|---|
| Headcount by ministry | `COUNT(DISTINCT pa_mat)` by `pa_codmin` | Staffing levels per ministry |
| Gender distribution | `pa_sexe` | Male/female ratio by ministry, grade, region |
| Average seniority | `pa_annee - YEAR(pa_datent)` | Years of service distribution |
| Grade distribution | `pa_grd` → `grade.CAT` | Pyramid of grade categories |
| Employment type mix | `pa_natu` → `nature.TYPNAT` | Civil servant vs. worker vs. contractual split |
| Family situation | `pa_codconj`, `pa_nbrfam`, `pa_enfits` | Dependents distribution for allowance planning |

### 3.2 Analyse des rémunérations

| Statistic | Description |
|---|---|
| Salary distribution | Histogram of `pa_netpay` — median, quartiles, outliers |
| Salary gap by gender | Compare `AVG(pa_netpay)` where `pa_sexe = 1` vs `pa_sexe = 2` |
| Salary by seniority curve | `pa_netpay` vs. `pa_eche` — echelon progression profile |
| Indemnity concentration | Top 10 most-paid indemnity codes from `indem_def` → Pareto analysis |
| Deduction burden | `(pa_salbrut - pa_netpay) / pa_salbrut` distribution — who bears the heaviest deductions? |

### 3.3 Analyse géographique

- **Heat map:** `SUM(pa_salbrut)` by `region.LIB_REG` on a Tunisia map
- **Cost per capita by governorate:** massa salariale / employee count per `region.CODGOUV`
- **Regional equity index:** compare average salary across regions for same grade

---

## 4. Détection d'anomalies — Abnormal Payroll Detection

### 4.1 Règles métier (Business Rules)

| Rule | Logic | Alert |
|---|---|---|
| Salary exceeds grade ceiling | `pa_salbrut > MAX_SALARY(pa_grd, pa_eche)` | Employee paid above their grade/echelon scale |
| Echelon out of range | `pa_eche > grade.FINREC` for the employee's `pa_grd` | Echelon exceeds maximum for this grade |
| Ghost employee | `pa_mat` exists in payroll but not in HR master | Payment to non-existent employee |
| Duplicate payment | Same `pa_mat` + `pa_mois` + `pa_annee` + `pa_type` appears twice | Double salary in same period |
| Retired but still paid | `AGE(pa_datnais) > grade.AGERET` and still receiving pay | Payment after retirement age |
| Zero salary with deductions | `pa_salbrut = 0` but `pa_cpe > 0` | Deductions without corresponding earnings |
| Net exceeds gross | `pa_netpay > pa_salbrut` | Net pay higher than gross — impossible without error |
| Negative deductions | `pa_cpe < 0` or `pa_retrait < 0` | Negative withholding — potential reversal or error |

### 4.2 Anomalies statistiques (Statistical Outliers)

| Method | Application | Detection |
|---|---|---|
| Z-score | `pa_netpay` per grade group | Employees whose salary is > 3σ from their grade mean |
| IQR (Interquartile Range) | `pa_sub` (indemnities) per ministry | Unusually high indemnity payments |
| Isolation Forest | Multi-feature: `pa_salbrut`, `pa_sub`, `pa_eche`, `pa_grd` | Unsupervised anomaly detection on full payroll profile |
| Time-series anomaly | `pa_netpay` per employee over months | Sudden salary spikes or drops for an individual |
| Benford's Law | First digit distribution of `pa_salbrut` | Detect fabricated or manipulated salary amounts |

### 4.3 Patterns suspects

| Pattern | Description | How to Detect |
|---|---|---|
| Salary without position | Employee has pay but no valid `pa_dire` / `pa_serv` in `organisme` | JOIN payroll ↔ organisme, flag unmatched |
| Indemnity mismatch | Employee receives indemnity code not applicable to their grade | Cross-check `indem_def.TMI_PFLAG` with `grade.CODGRD` |
| Unusual payment type | `pa_type = 4` (supplementary) appearing every month | PAICOMP should be occasional, not regular |
| Bank account reuse | Same `pa_idbank` for multiple `pa_mat` | Multiple employees paid to the same bank account |
| Rapid echelon jump | `pa_eche` increases by > 1 between consecutive months | Abnormal advancement speed |
| Post-mortem payment | Employee flagged deceased (`pa_capdeces` paid out) but still receiving salary in later months | Timeline check after death benefit |

---

## 5. KPIs & Tableau de bord proposé

### Executive Dashboard

```
┌─────────────────────────────────────────────────────────────┐
│  MASSE SALARIALE GLOBALE          │  EFFECTIFS TOTAUX       │
│  ████████████  1.2 Mrd DT         │  ██████  45,230         │
│  ▲ +3.2% vs année précédente      │  ▲ +1.1% vs N-1        │
├─────────────────────────────────────────────────────────────┤
│  COÛT MOYEN / AGENT   │  TAUX DE CHARGES  │  ANOMALIES     │
│  2,150 DT/mois        │  28.4%            │  ⚠ 127 alertes │
├─────────────────────────────────────────────────────────────┤
│  TOP 5 MINISTÈRES (coût)  │  RÉPARTITION PAR GRADE          │
│  1. Santé      320M DT    │  Cat 1-2: 35%  ████             │
│  2. Intérieur  280M DT    │  Cat 3-4: 45%  █████            │
│  3. Éducation  250M DT    │  Cat 5-6: 20%  ██               │
│  4. Défense    180M DT    │                                  │
│  5. Ens. Sup.  120M DT    │                                  │
├─────────────────────────────────────────────────────────────┤
│  PRÉVISION N+1             │  DÉPARTS RETRAITE PRÉVUS        │
│  1.24 Mrd DT (+3.5%)      │  2026: 1,230  │  2027: 1,450   │
│  ─────────────────────▶    │  2028: 1,680  │  2029: 1,520   │
└─────────────────────────────────────────────────────────────┘
```

### Anomaly Monitoring Dashboard

```
┌─────────────────────────────────────────────────────────────┐
│  ALERTES ACTIVES: 127       │  SÉVÉRITÉ                     │
│  ● Critique:  12            │  🔴 Doublons de paiement (8)  │
│  ● Haute:     35            │  🔴 Retraités payés (4)       │
│  ● Moyenne:   80            │  🟡 Échelon hors plafond (35) │
│                             │  🟡 Salaire > seuil grade (22)│
│                             │  ⚪ Outliers statistiques (58) │
├─────────────────────────────────────────────────────────────┤
│  TENDANCE ANOMALIES / MOIS  │  TOP MINISTÈRES CONCERNÉS     │
│  Jan ██████  45              │  Santé: 42 alertes            │
│  Fév ████    32              │  Intérieur: 31 alertes        │
│  Mar ████████ 50             │  Éducation: 28 alertes        │
└─────────────────────────────────────────────────────────────┘
```

---

## 6. Architecture BI proposée

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────┐
│  JSON Files  │────▶│   ETL Layer  │────▶│  Data Warehouse  │
│  (Sources)   │     │  (Staging)   │     │  (Star Schema)   │
└──────────────┘     └──────────────┘     └────────┬─────────┘
                                                   │
              ┌────────────────────────────────────┤
              │                                    │
     ┌────────▼────────┐              ┌────────────▼──────────┐
     │   OLAP Cubes    │              │   ML / Analytics      │
     │   (Analysis)    │              │   (Python / R)        │
     └────────┬────────┘              └────────────┬──────────┘
              │                                    │
     ┌────────▼────────┐              ┌────────────▼──────────┐
     │   Dashboards    │              │   Anomaly Detection   │
     │ (Power BI /     │              │   & Forecasting       │
     │  Tableau)       │              │   Reports             │
     └─────────────────┘              └───────────────────────┘
```

### Dimension Tables (from reference data)

| Dimension | Source | Key |
|---|---|---|
| `dim_grade` | `grade.json` | `CODGRD` |
| `dim_nature` | `nature.json` | `CODNAT` |
| `dim_region` | `region.json` | `CODREG` |
| `dim_organisme` | `organisme.json` | `CODETAB + DIRE` |
| `dim_indemnite` | `indem_def.json` | `TMI_CIND` |
| `dim_temps` | Generated | `pa_mois + pa_annee` |

### Fact Tables (from payroll data)

| Fact | Source | Grain |
|---|---|---|
| `fact_paie` | `schema-paie.json` | One row per employee per month per pay type |
| `fact_indemnite` | `schema-indemnity.json` | One row per employee per month per indemnity |

---

## 7. Outils recommandés

| Layer | Tool Options |
|---|---|
| ETL / Data Pipeline | Python (Pandas), Talend, Apache NiFi |
| Data Warehouse | PostgreSQL, MySQL, SQL Server |
| OLAP / Analysis | Power BI, Tableau, Apache Superset |
| Machine Learning | Python (scikit-learn, Prophet, statsmodels) |
| Anomaly Detection | Isolation Forest (sklearn), PyOD, custom rules engine |
| Reporting | Power BI dashboards, Jupyter notebooks |
