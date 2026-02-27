# Nature (Employee Classification) Schema Documentation

> **Source:** `nature.json`
> **System:** French-language government payroll system — employee classification reference table
> **Format:** Lookup table with `columns` and `items` arrays

---

## Overview

The `nature.json` file defines the **types of employment** (nature de l'acte) used to classify employees in the payroll system. Each employee record references a `pa_natu` code that maps to one of these categories.

---

## Table Structure

| Column | Type | Full Name (French) | Description |
|---|---|---|---|
| `CODNAT` | VARCHAR2 | Code Nature | **Nature code** — unique identifier for the employment type (maps to `pa_natu` in payroll records) |
| `TYPNAT` | VARCHAR2 | Type Nature | **Nature type** — groups employment types into broader categories: `1` = Civil servants, `2` = Workers, `3` = Contractual/Other |
| `LIBNATL` | VARCHAR2 | Libellé Nature (Latin) | **Label (French)** — French-language name for the employment type |
| `LIBNATA` | VARCHAR2 | Libellé Nature (Arabe) | **Label (Arabic)** — Arabic-language name for the employment type |

---

## Employment Categories

### Type `1` — Civil Servants (Fonctionnaires)

| Code | French Label | Description |
|---|---|---|
| `1` | FONCTIONNAIRE STAGIAIRE | **Probationary civil servant** — employee on probation period |
| `2` | FONCTIONNAIRE TITULAIRE | **Tenured civil servant** — permanent civil service employee |
| `A` | CONTRACTUEL ASSIMILE | **Assimilated contractual** — contractor treated as equivalent to civil servant |

### Type `2` — Workers (Ouvriers)

| Code | French Label | Description |
|---|---|---|
| `3` | OUVRIER TITULAIRE | **Tenured worker** — permanent worker |
| `4` | OUVRIER STAGIAIRE | **Probationary worker** — worker on probation |
| `5` | OUVRIER TEMPORAIRE | **Temporary worker** — worker on a fixed-term basis |
| `6` | OUVRIER OCCASIONNEL | **Occasional worker** — seasonal or occasional worker |

### Type `3` — Contractual & Other

| Code | French Label | Description |
|---|---|---|
| `7` | AGENT TEMPORAIRE | **Temporary agent** — non-permanent employee |
| `8` | CONTRACTUEL | **Contractual employee** — worker under contract |
| `9` | DIVERS | **Miscellaneous** — other types of employment |
| `B` | AGENT TEMPORAIRE ASSIMILE | **Assimilated temporary agent** — temporary agent treated as equivalent rank |
| `C` | OMDA | **Omda** — local administrative chief (traditional role) |

---

## Usage in Payroll

The `CODNAT` value is stored in the `pa_natu` field of payroll records (`schema-paie.json` and `schema-indemnity.json`). It determines:
- Pension and retirement fund eligibility
- Applicable deduction rules
- Contract type classification for HR and legal purposes
