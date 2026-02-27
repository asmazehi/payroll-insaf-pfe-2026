# Indemnity Definitions (Allowance Catalog) Schema Documentation

> **Source:** `indem_def.json`
> **System:** French-language government payroll system — indemnity/allowance definitions
> **Format:** Lookup table with `columns` and `items` arrays
> **Size:** ~248 KB, 100+ indemnity type entries

---

## Overview

The `indem_def.json` file defines the **catalog of all allowances and indemnities** that can be paid to government employees. Each entry specifies the indemnity code, calculation parameters, tax treatment, and descriptive labels. These definitions are used to compute the indemnity portions of payroll records (type `3`).

---

## Table Structure

| Column | Type | Full Name (French) | Description |
|---|---|---|---|
| `TMI_CIND` | DATE* | Code Indemnité | **Indemnity code** — unique identifier for the allowance type (4 chars, zero-padded) |
| `TMI_ARG1` | NUMBER | Argument 1 | **Calculation parameter 1** — primary lookup key for amount calculation |
| `TMI_ARG2` | NUMBER | Argument 2 | **Calculation parameter 2** — secondary lookup key (often `0`) |
| `TMI_NSEQ` | NUMBER | Numéro Séquence | **Sequence number** — ordering sequence for calculation priority |
| `TMI_NVAL` | NUMBER | Nombre Valeurs | **Number of values** — count of value entries for this indemnity |
| `TMI_CNR` | DATE* | CNR | **CNR flag** — `1` = counts towards CNR (retirement fund), `0` = excluded |
| `TMI_IMP` | DATE* | Imposable | **Taxable flag** — `1` = taxable, `0` = tax-exempt |
| `TMI_FIL1` | DATE* | Filtre 1 | **Filter 1** — date-based filter for applicability (start) |
| `TMI_FIL2` | DATE* | Filtre 2 | **Filter 2** — date-based filter for applicability (end) |
| `TMI_NAT` | DATE* | Nature | **Nature** — `F` = fixed, `V` = variable, `A` = automatic |
| `TMI_NAI` | DATE* | Nature Indemnité | **Indemnity nature** — `0` = standard, `1` = security-related, `2` = grade-dependent |
| `TMI_PFLAG` | NUMBER | Paramètre Flag | **Parameter flag** — calculation mode or special processing flag |
| `TMI_DPC` | — | Date Prise en Charge | **Effective date** — date from which this indemnity definition applies |
| `TMI_ZON` | DATE* | Zone | **Zone** — `C` = central, `N` = non-central |
| `TMI_LIBC` | DATE* | Libellé Court | **Short label** — abbreviated French name (max ~12 chars) |
| `TMI_LIBL` | DATE* | Libellé Long | **Full label** — complete French description |
| `TMI_CINS` | DATE* | Code Indemnité (Short) | **Short indemnity code** — 3-character version without leading zero |
| `TMI_LIBA` | DATE* | Libellé (Arabe) | **Label (Arabic)** — Arabic description |

> *Note: Some columns have `DATE` type in the schema metadata but actually store string/code values.

---

## Indemnity Categories

### Base Salary & Core Compensation

| Code | Short Label | Full Label | Taxable | CNR |
|---|---|---|---|---|
| `0001` | TRAIT. BASE | Traitement de base (Base salary) | Yes | Yes |
| `0101` | RAP. IND. | Rappel indemnités (Back pay) | Yes | Yes |
| `0303` | IND. REND. | Indemnité de rendement (Performance bonus) | Yes | Yes |
| `0320` | PRIME RENDMT | Prime de rendement (Performance premium) | Yes | Yes |

### Function & Responsibility Allowances

| Code | Short Label | Full Label | Taxable | CNR |
|---|---|---|---|---|
| `0425` | RAP. FONC. | Rappel indemnité liée à la fonction | Yes | Yes |
| `0463` | FONCTION | Indemnité de fonction (Function allowance) | Yes | Yes |
| `0462` | RESP/COMMAND | Indemnité de responsabilité et commandement | Yes | Yes |
| `0467` | REPRESENT. | Indemnité de représentation | Yes | Yes |

### Housing & Transport

| Code | Short Label | Full Label | Taxable | CNR |
|---|---|---|---|---|
| `0306` | IND. LOG. | Indemnité de logement (Housing allowance) | Yes | Yes |
| `0384` | IND LOG | Indemnité de logement | Yes | Yes |
| `0700` | IND. KILOM. | Indemnité kilométrique (Mileage allowance) | Yes | Yes |

### Risk & Hazard Pay

| Code | Short Label | Full Label | Taxable | CNR |
|---|---|---|---|---|
| `0222` | RISQUE CONTA | Indemnité risque de contagion | Yes | Yes |
| `0654` | RISQ.DEMIN. | Indemnité risque démineur (Mine clearance) | Yes | Yes |
| `0655` | RISQUE ARTIF | Indemnité risque artificier (Explosives) | Yes | Yes |
| `0380` | IND. SAHARA | Indemnité de Sahara (Desert posting) | Yes | Yes |

### Special & Professional Allowances

| Code | Short Label | Full Label | Taxable | CNR |
|---|---|---|---|---|
| `0022` | GEOLOGIE | Indemnité de géologie | Yes | Yes |
| `0323` | PSYCHOLOGIE | Indemnité de psychologie | Yes | Yes |
| `0404` | IND. SPECIAL | Indemnité de spécialisation | Yes | Yes |
| `0718` | SERV. SANIT | Indemnité services sanitaires | Yes | Yes |
| `0405` | SERVICE HOSP | Indemnité service hospitalier | Yes | Yes |

### Military-Specific Allowances

| Code | Short Label | Full Label | Taxable | CNR |
|---|---|---|---|---|
| `0005` | COMP.COMPL.M | Indemnité compensatoire complémentaire militaire | Yes | Yes |
| `0645` | COMMANDOS | Indemnité commandos | Yes | Yes |
| `0649` | INT.NAGE 1 D | Indemnité intervention nage combat 1er degré | Yes | Yes |
| `0685` | TECH.MILIT. | Indemnité de technicité (Military) | Yes | Yes |
| `0715` | FONCT. MILIT | Indemnité fonction militaire | Yes | Yes |

### Working Conditions

| Code | Short Label | Full Label | Taxable | CNR |
|---|---|---|---|---|
| `0432` | IND ISOLEMENT | Indemnité d'isolement | Yes | Yes |
| `0530` | TRAVAIL NUIT | Indemnité travail de nuit (Night work) | Yes | No |
| `0401` | PLEIN TEMPS | Indemnité plein temps (Full-time) | Yes | Yes |
| `0522` | IND.H/S TD | Indemnité heures supplémentaires (Overtime) | Yes | Yes |

### Family & Social

| Code | Short Label | Full Label | Taxable | CNR |
|---|---|---|---|---|
| `0590` | MAJOR. A.FAM | Majoration allocation familiale (Family bonus) | Yes | Yes |
| `0372` | — | Indemnité d'accouchement (Maternity) | Yes | Yes |

---

## Key Flags Explained

| Field | Value | Meaning |
|---|---|---|
| `TMI_CNR` | `1` | Counts towards retirement fund (CNR) base |
| `TMI_CNR` | `0` | Excluded from retirement fund calculation |
| `TMI_IMP` | `1` | Subject to income tax |
| `TMI_IMP` | `0` | Tax-exempt |
| `TMI_NAT` | `F` | Fixed amount (based on grade/function) |
| `TMI_NAT` | `V` | Variable amount |
| `TMI_NAT` | `A` | Automatic calculation |
| `TMI_ZON` | `C` | Central zone |
| `TMI_ZON` | `N` | Non-central zone |

---

## Usage in Payroll

Indemnity definitions are used during payroll processing to:
- Look up the correct allowance amount based on employee grade, function, and parameters
- Determine whether each indemnity is taxable (`TMI_IMP`) or counts for retirement (`TMI_CNR`)
- Calculate the indemnity totals that feed into `pa_salimp` (taxable) and `pa_salnimp` (non-taxable) in payroll records
- Apply date-based filters to ensure indemnities are only paid during their validity period
