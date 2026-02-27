# Grade (Rank & Classification) Schema Documentation

> **Source:** `grade.json`
> **System:** French-language government payroll system — grade/rank reference table
> **Format:** Lookup table with `columns` and `items` arrays
> **Size:** ~710 KB, 500+ grade entries

---

## Overview

The `grade.json` file defines the **civil service grades and ranks** used across all government ministries. Each employee record references a `pa_grd` code that maps to one of these grade definitions. Grades determine salary scales, promotion paths, and retirement conditions.

---

## Table Structure

| Column | Type | Full Name (French) | Description |
|---|---|---|---|
| `CODGRD` | VARCHAR2 | Code Grade | **Grade code** — unique identifier (maps to `pa_grd` in payroll records) |
| `CODCORPS` | VARCHAR2 | Code Corps | **Corps code** — the professional body or corps the grade belongs to |
| `CAT` | VARCHAR2 | Catégorie | **Category** — civil service category (`1`–`9`, where `1` = highest rank) |
| `CLASSGRD` | VARCHAR2 | Classe Grade | **Grade class** — classification level within the category |
| `EFONC` | VARCHAR2 | Échelon Fonctionnel | **Functional step** — additional step tied to responsibilities |
| `LIBCGRDL` | VARCHAR2 | Libellé Court Grade (Latin) | **Short label (French)** — abbreviated grade name |
| `LIBCGRDA` | VARCHAR2 | Libellé Court Grade (Arabe) | **Short label (Arabic)** — abbreviated grade name in Arabic |
| `LIBLGRDL` | VARCHAR2 | Libellé Long Grade (Latin) | **Full label (French)** — complete grade name |
| `LIBLGRDA` | VARCHAR2 | Libellé Long Grade (Arabe) | **Full label (Arabic)** — complete grade name in Arabic |
| `AGEMAX` | VARCHAR2 | Âge Maximum | **Maximum age** — upper age limit for recruitment into this grade |
| `AGEMIN` | VARCHAR2 | Âge Minimum | **Minimum age** — lower age limit for recruitment |
| `GPROM` | VARCHAR2 | Grade Promotion | **Promotion type** — `G` = general promotion, `P` = promotion by selection |
| `TYPGRD` | VARCHAR2 | Type Grade | **Grade type** — `G` = general grade |
| `AGERET` | VARCHAR2 | Âge Retraite | **Retirement age** — mandatory retirement age for this grade |
| `DEFGI` | — | Date Effet Grade (Initiale) | **Grade effective date (initial)** — when the grade was first established |
| `DEFGV` | — | Date Effet Grade (Vigueur) | **Grade effective date (current)** — when the current definition took effect |
| `HCORPS` | VARCHAR2 | Hiérarchie Corps | **Corps hierarchy** — hierarchical ranking code within the corps |
| `ETAT_G` | VARCHAR2 | État Grade | **Grade status** — active/inactive status |
| `NATREM` | VARCHAR2 | Nature Rémunération | **Remuneration nature** — `V` = special salary scale (variable) |
| `FINREC` | VARCHAR2 | Fin Recrutement | **Recruitment end** — echelon limit for new recruits |
| `FIN200` | VARCHAR2 | Fin 200 | **200 limit** — echelon cap for the 200-point scale |
| `NIVDEP` | VARCHAR2 | Niveau Départ | **Starting level** — starting echelon for new appointees to this grade |

---

## Grade Categories

| Category | Level | Example Grades |
|---|---|---|
| `1` | Senior / Executive | Generals, Colonels, Directors, Professors, Senior Doctors |
| `2` | Professional / Officer | Lieutenants, Captains, Inspectors, Engineers, Commissioners |
| `3` | Technical / Mid-level | Technicians, Lab Chiefs, Nurses, Pilots |
| `4` | Administrative | Secretaries, Controllers, Attachés, Masters |
| `5` | Support / Clerical | Clerks, Commis, Agents, Sergeants |
| `6` | Entry-level / Manual | Typists, Auxiliaries, Corporals, Laborers |
| `8`–`9` | Workers | Workers categories 3–4 |

---

## Example Grades

| Code | French Label | Corps | Category | Retirement Age |
|---|---|---|---|---|
| `0T1` | Professeur de l'enseignement supérieur | 42 | 1 | 65 |
| `09F` | Commissaire de Police | 32 | 2 | 57 |
| `0DP` | Infirmier spécialisé de la santé publique | ZZ | 3 | 60 |
| `094` | Commis d'administration | 02 | 5 | 62 |
| `08D` | Colonel | 20 | 1 | 62 |

---

## Usage in Payroll

The `CODGRD` value is stored in the `pa_grd` field of payroll records. It determines:
- Base salary calculation through the salary scale
- Applicable echelon range (`pa_eche`)
- Retirement age and pension eligibility
- Promotion paths and career progression
