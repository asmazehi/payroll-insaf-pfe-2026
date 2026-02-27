# Payroll Schema Documentation

> **Source:** `schema.json`
> **System:** French-language government payroll system
> **Prefix:** `pa_` = "paie" (French for "payroll")

---

## Employee Identification

| Field | Full Name (French) | Description | Example |
|---|---|---|---|
| `pa_codmin` | Code Ministère | **Ministry code** — identifies the government ministry the employee belongs to | `"S00"` |
| `pa_mat` | Matricule | **Employee ID / registration number** — unique identifier for the employee | `"2291741194"` |
| `pa_noml` | Nom légal | **Last name** of the employee | `"FRANCO"` |
| `pa_prenl` | Prénom légal | **First name(s)** of the employee | `"MARIO FERNANDO"` |
| `pa_sexe` | Sexe | **Gender** — `1` = Male, `2` = Female | `"1"` |
| `pa_datnais` | Date de naissance | **Date of birth** (DD/MM/YY format) | `"09/01/61"` → Jan 9, 1961 |
| `pa_datent` | Date d'entrée | **Hire date / date of entry** into service (DD/MM/YY) | `"12/09/10"` → Sep 12, 2010 |

---

## Pay Period

| Field | Full Name (French) | Description | Example |
|---|---|---|---|
| `pa_mois` | Mois | **Pay month** (1–12) | `1` (January) |
| `pa_annee` | Année | **Pay year** | `2016` |
| `pa_type` | Type de paie | **Payroll type** — categorizes the type of payment (e.g., `1` = regular salary) | `"1"` |

---

## Organizational Structure

| Field | Full Name (French) | Description | Example |
|---|---|---|---|
| `pa_dg` | Direction Générale | **General directorate** code | `"000"` |
| `pa_dire` | Direction | **Directorate / department** code | `"S00"` |
| `pa_sdir` | Sous-Direction | **Sub-directorate** code | `"000"` |
| `pa_serv` | Service | **Service / unit** code | `"000"` |

---

## Grade & Classification

| Field | Full Name (French) | Description | Example |
|---|---|---|---|
| `pa_article` | Article budgétaire | **Budget article** — budget line item associated with the position | `"01136"` |
| `pa_grd` | Grade | **Grade / rank** of the employee in the civil service hierarchy | `"03F"` |
| `pa_eche` | Échelon | **Step / echelon** — seniority level within the grade | `1` |
| `pa_natu` | Nature de l'acte | **Nature of appointment** — type of employment action (e.g., `8` = integration, contract, etc.) | `"8"` |
| `pa_datnatu` | Date de l'acte | **Date of the appointment action** (DD/MM/YY) | `"12/09/10"` |

---

## Salary & Compensation (Earnings)

| Field | Full Name (French) | Description | Example |
|---|---|---|---|
| `pa_salimp` | Salaire imposable | **Taxable salary** — portion of salary subject to income tax | `1923.452` |
| `pa_salnimp` | Salaire non imposable | **Non-taxable salary** — portion of salary exempt from tax | `0` |
| `pa_salbrut` | Salaire brut | **Gross salary** — total salary before any deductions | `2184.5` |
| `pa_sub` | Subventions / Indemnités | **Allowances / subsidies** — additional benefits or allowances | `273.063` |
| `pa_rapimp` | Rappel imposable | **Taxable back pay** — retroactive taxable salary adjustments | `0` |
| `pa_rapni` | Rappel non imposable | **Non-taxable back pay** — retroactive non-taxable adjustments | `0` |
| `pa_rapsalb` | Rappel salaire brut | **Gross salary back pay** — retroactive gross salary adjustment | `0` |

---

## Deductions

| Field | Full Name (French) | Description | Example |
|---|---|---|---|
| `pa_cpe` | Cotisation patronale et employé | **Employee tax / contributions** — income tax and social contributions withheld | `354.683` |
| `pa_retrait` | Retenue retraite | **Pension / retirement contribution** deducted from salary | `179.129` |
| `pa_cps` | Cotisation de prévoyance sociale | **Social security contribution** — social welfare fund deduction | `60.074` |
| `pa_capdeces` | Capital décès | **Death benefit insurance** — contribution to a death/life insurance fund | `21.845` |
| `pa_sps` | Sécurité / Protection sociale | **Social protection contribution** — employer/employee social protection levy | `87.38` |
| `pa_spl` | Supplément / prélèvement | **Supplementary levy** — additional mandatory deduction | `21.845` |

---

## Net Pay

| Field | Full Name (French) | Description | Example |
|---|---|---|---|
| `pa_netord` | Net à ordonnancer | **Net pay to be authorized** — the amount approved for payment processing | `1568.769` |
| `pa_netpay` | Net à payer | **Net pay** — the final amount the employee actually receives | `1568.769` |
| `pa_brutcnr` | Brut CNR | **Gross for national retirement fund (CNR)** — base used for calculating retirement fund contributions | `2184.5` |

---

## Summary Formula

```
Gross Salary (pa_salbrut)  =  Taxable Salary (pa_salimp) + Allowances (pa_sub) - adjustments
Net Pay (pa_netpay)        =  Gross Salary - Tax (pa_cpe) - Pension (pa_retrait)
                              - Social Security (pa_cps) - Death Insurance (pa_capdeces)
                              - Other deductions
```

**Example:** `2184.50 (gross) → 1568.77 (net)` with total deductions of **615.73**.
