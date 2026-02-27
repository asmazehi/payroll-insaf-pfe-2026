# Organisme (Organization Structure) Schema Documentation

> **Source:** `organisme.json`
> **System:** French-language government payroll system — organizational structure reference table
> **Format:** Lookup table with `columns` and `items` arrays
> **Size:** ~77 KB, 90+ organization entries

---

## Overview

The `organisme.json` file defines the **organizational structure** of government ministries and their sub-divisions. Each entry represents a directorate, service, unit, or governorate. This table is used to resolve the organizational hierarchy codes (`pa_cab`, `pa_sg`, `pa_dg`, `pa_dire`, `pa_sdir`, `pa_serv`, `pa_unite`) found in payroll records.

---

## Table Structure

| Column | Type | Full Name (French) | Description |
|---|---|---|---|
| `CODETAB` | VARCHAR2 | Code Établissement | **Establishment code** — ministry/department code (e.g., `F00` = Interior, `H00` = Health) |
| `CAB` | CHAR | Cabinet | **Cabinet** code — ministerial cabinet level |
| `SG` | CHAR | Secrétariat Général | **General Secretariat** code |
| `DG` | CHAR | Direction Générale | **General Directorate** code |
| `DIRE` | CHAR | Direction | **Directorate** code — primary organizational unit |
| `SDIR` | CHAR | Sous-Direction | **Sub-directorate** code |
| `SERV` | CHAR | Service | **Service** code |
| `UNITE` | CHAR | Unité | **Unit** code — smallest organizational unit |
| `LIBORGL` | VARCHAR2 | Libellé Organisation (Latin) | **Organization name (French)** |
| `LIBORGA` | VARCHAR2 | Libellé Organisation (Arabe) | **Organization name (Arabic)** |
| `TYPSTRUCT` | VARCHAR2 | Type Structure | **Structure type** — `A` = administrative |
| `CODLOC` | VARCHAR2 | Code Localisation | **Location code** — physical location reference |
| `ETATDORG` | VARCHAR2 | État de l'Organisation | **Organization status** — active/inactive |
| `ROLEORGL` | VARCHAR2 | Rôle Organisation (Latin) | **Organization role (French)** |
| `ROLEORGA` | VARCHAR2 | Rôle Organisation (Arabe) | **Organization role (Arabic)** |
| `DELEG` | VARCHAR2 | Délégation | **Delegation flag** — `O` = has delegations, `N` = no delegations |
| `CODGOUV` | VARCHAR2 | Code Gouvernorat | **Governorate code** — links to the governorate (e.g., `X` = central, `C` = Ariana, `B` = Bizerte) |
| `CENTREG` | VARCHAR2 | Centre Régional | **Regional center** — `R` = regional center |
| `GBOPRG` | VARCHAR2 | GBO Programme | **Budget program** — GBO (Gestion Budgétaire par Objectifs) program code |
| `GBOSPRG` | VARCHAR2 | GBO Sous-Programme | **Budget sub-program** — GBO sub-program code |
| `GBOACT` | VARCHAR2 | GBO Activité | **Budget activity** — GBO activity code |
| `GBOUO` | VARCHAR2 | GBO Unité Opérationnelle | **Budget operational unit** — GBO operational unit code |

---

## Organization Types

### Central Administration (Example: Ministry of Interior — `F00`)

| Directorate Code | French Name | Description |
|---|---|---|
| `001` | CABINET | Ministerial cabinet |
| `354` | SECRETARIAT GENERAL | General secretariat |
| `006` | DIR. GEN. DES AFFAIRES ADMIN. ET FINANCIERES | General directorate of admin & financial affairs |
| `060` | DIR. GENERALE DE L'INFORMATIQUE | General directorate of IT |
| `120` | D.G DE LA SURETE NATIONALE | General directorate of national security |
| `150` | D.G DE LA GARDE NATIONALE | General directorate of national guard |

### Governorates (Regional Administration)

| Directorate Code | French Name | Governorate Code |
|---|---|---|
| `0BA` | GOUVERNORAT DE L'ARIANA | C |
| `0BB` | GOUVERNORAT DE TUNIS | X |
| `0BD` | GOUVERNORAT DE BIZERTE | B |
| `0BS` | GOUVERNORAT DE SFAX | R |
| `0BW` | GOUVERNORAT DE SOUSSE | V |
| `0BY` | GOUVERNORAT DE MANOUBA | M |

### Health Ministry Directorates (`H00`)

| Directorate Code | French Name |
|---|---|
| `001` | CABINET |
| `002` | D.G DES SERVICES COMMUNS |
| `010` | D.G DES STRUCTURES DE LA SANTE PUBLIQUES |

---

## Hierarchical Key

Organizations are uniquely identified by the combination of:

```
CODETAB + CAB + SG + DG + DIRE + SDIR + SERV + UNITE
```

This matches the payroll fields: `pa_codmin` + `pa_cab` + `pa_sg` + `pa_dg` + `pa_dire` + `pa_sdir` + `pa_serv` + `pa_unite`.

---

## Usage in Payroll

The organizational codes map directly to payroll record fields:
- `pa_dire` → `DIRE` — identifies the employee's directorate
- `pa_sdir` → `SDIR` — identifies the sub-directorate
- `pa_serv` → `SERV` — identifies the service
- Enables organizational reporting and budget tracking via GBO codes
