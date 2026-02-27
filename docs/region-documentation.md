# Region (Location Reference) Schema Documentation

> **Source:** `region.json`
> **System:** French-language government payroll system — regional/location reference table
> **Format:** Lookup table with `columns` and `items` arrays
> **Size:** ~27 KB, 138+ region entries

---

## Overview

The `region.json` file defines the **regional locations and facilities** where government employees are assigned. Each entry represents a hospital, university, regional health administration, or other government facility. This table is referenced by `pa_loca` and organizational codes in payroll records.

---

## Table Structure

| Column | Type | Full Name (French) | Description |
|---|---|---|---|
| `CODDEP` | VARCHAR2 | Code Département | **Department code** — ministry/department code (e.g., `H00` = Health, `S00` = Higher Education) |
| `CODREG` | VARCHAR2 | Code Région | **Region code** — unique identifier for the specific location/facility |
| `LIB_REG` | VARCHAR2 | Libellé Région | **Region label (French)** — short name or file reference for the location |
| `LIB_REGA` | VARCHAR2 | Libellé Région (Arabe) | **Region label (Arabic)** — Arabic name of the location |
| `CODE_DEPT` | VARCHAR2 | Code Département (alt) | **Department code** — alternate department reference (usually same as `CODDEP`) |
| `CODE_REGION` | VARCHAR2 | Code Région (alt) | **Region code (alt)** — parent region code for sub-facilities |
| `FICHIER` | VARCHAR2 | Fichier | **File name** — reference file name associated with this location's payroll data |
| `CODSREG` | VARCHAR2 | Code Sous-Région | **Sub-region code** — sub-regional classification (`0` = not applicable) |

---

## Department Breakdown

### `H00` — Ministry of Health (Ministère de la Santé)

The majority of entries belong to the health sector, including:

**University Hospitals (CHU):**

| Region Code | French Name | Description |
|---|---|---|
| `0HV` | hmongislim | Hôpital Mongi Slim (La Marsa) |
| `0HW` | hbourguiba | Hôpital Habib Bourguiba (Sfax) |
| `0HX` | hthameur | Hôpital Habib Thameur |
| `0I0` | hfattouma | Hôpital Fattouma Bourguiba (Monastir) |
| `0I3` | hsahloul | Hôpital Sahloul (Sousse) |
| `0I4` | hrabta | Hôpital La Rabta |
| `0I5` | hcharlenicol | Hôpital Charles Nicolle |

**Specialized Institutes:**

| Region Code | French Name | Description |
|---|---|---|
| `0HQ` | iophtalmo | Institut Hédi Raïs d'Ophtalmologie |
| `0HR` | ineurologie | Institut National de Neurologie |
| `0HS` | isalahazziz | Institut Salah Azaiez (Oncology) |
| `0HT` | inutrition | Institut National de Nutrition |
| `0HU` | iorthopedi | Institut M.T. Kassab d'Orthopédie |

**Regional Health Administrations (Directions Régionales):**

| Region Code | French Name | Governorate |
|---|---|---|
| `0AA` | aariana | Ariana |
| `0AB` | abeja | Béja |
| `0AD` | abizerte | Bizerte |
| `0AE` | agabes | Gabès |
| `0AF` | agafsa | Gafsa |
| `0AR` | asfax | Sfax |
| `0AY` | atunis | Tunis |

### `S00` — Ministry of Higher Education (Ministère de l'Enseignement Supérieur)

| Region Code | French Name | Description |
|---|---|---|
| `04C` | atunis | Université de Tunis |
| `04D` | amanar | Université de Tunis El Manar |
| `04E` | acarthage | Université de Carthage |
| `04F` | amanouba | Université de la Manouba |
| `256` | acentre | Université de Sousse |
| `254` | asfax | Université de Sfax |
| `166` | azitouna | Université Zitouna |
| `04H` | agabes | Université de Gabès |

---

## Usage in Payroll

Region codes are referenced indirectly through the organizational structure in payroll records. They help determine:
- Employee work location
- Regional allowances (e.g., isolation bonuses for remote areas)
- Payroll file routing and processing
- Governorate-level reporting
