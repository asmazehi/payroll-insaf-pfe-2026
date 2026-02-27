# INSAF — Payroll Intelligence Platform

> **PFE 2026** — Business Intelligence applied to government payroll data (masse salariale)

---

## Objectif du projet

Conception et mise en place d'une plateforme BI pour l'analyse de la **masse salariale** du secteur public, couvrant :

- **Analyse descriptive** — tableaux de bord de la masse salariale par ministère, grade, région et type d'emploi
- **Prédiction** — projection budgétaire, simulation de scénarios (augmentations, recrutements, départs en retraite)
- **Détection d'anomalies** — identification automatique des paies anormales, doublons, employés fantômes et incohérences

---

## Données sources

| Fichier | Description | Taille |
|---|---|---|
| `schema-paie.json` | Fiche de paie mensuelle (type 1 — PAIE) | Exemple unitaire |
| `schema-indemnity.json` | Fiche d'indemnités (type 3 — INDCOMP) | Exemple unitaire |
| `grade.json` | Référentiel des grades et rangs de la fonction publique | ~500 grades |
| `nature.json` | Types d'emploi (fonctionnaire, ouvrier, contractuel...) | 12 types |
| `region.json` | Localisations (hôpitaux, universités, administrations régionales) | 138+ sites |
| `organisme.json` | Structure organisationnelle (directions, services, gouvernorats) | 90+ unités |
| `indem_def.json` | Catalogue des indemnités et primes | 100+ types |

### Types de paie (`pa_type`)

| Code | Français | العربية | Description |
|---|---|---|---|
| `1` | PAIE | خلاص شهري | Salaire mensuel |
| `2` | PRIME | منحة إنتاج | Prime de rendement |
| `3` | INDCOMP | منحة خاصة | Indemnités spéciales |
| `4` | PAICOMP | خلاص شهري تكميلي | Salaire complémentaire |

---

## Documentation

### Schémas de données

| Document | Contenu |
|---|---|
| [schema-paie-documentation.md](docs/schema-paie-documentation.md) | Tous les champs de la fiche de paie (56 champs) |
| [schema-indemnity-documentation.md](docs/schema-indemnity-documentation.md) | Tous les champs de la fiche d'indemnités |
| [grade-documentation.md](docs/grade-documentation.md) | Structure du référentiel des grades (22 colonnes) |
| [nature-documentation.md](docs/nature-documentation.md) | Classification des types d'emploi |
| [region-documentation.md](docs/region-documentation.md) | Référentiel des localisations géographiques |
| [organisme-documentation.md](docs/organisme-documentation.md) | Structure organisationnelle hiérarchique |
| [indem-def-documentation.md](docs/indem-def-documentation.md) | Catalogue des indemnités avec paramètres de calcul |

### Cas d'usage BI

| Document | Contenu |
|---|---|
| [bi-use-cases.md](docs/bi-use-cases.md) | Cas d'usage détaillés : masse salariale, prédiction, statistiques, détection d'anomalies, architecture BI |

---

## Architecture cible

```
  JSON Sources          ETL              Data Warehouse           Restitution
 ┌────────────┐    ┌──────────┐    ┌───────────────────┐    ┌─────────────────┐
 │ schema-paie│───▶│          │───▶│   fact_paie        │───▶│   Dashboards    │
 │ schema-ind.│───▶│  Python  │───▶│   fact_indemnite   │    │   (Power BI)    │
 ├────────────┤    │  Pandas  │    ├───────────────────┤    ├─────────────────┤
 │ grade.json │───▶│          │───▶│   dim_grade        │───▶│   Prédiction    │
 │ nature.json│───▶│          │───▶│   dim_nature       │    │   (Prophet)     │
 │ region.json│───▶│          │───▶│   dim_region       │    ├─────────────────┤
 │ organisme  │───▶│          │───▶│   dim_organisme    │───▶│   Anomalies     │
 │ indem_def  │───▶│          │───▶│   dim_indemnite    │    │   (Isolation    │
 └────────────┘    └──────────┘    │   dim_temps        │    │    Forest)      │
                                   └───────────────────┘    └─────────────────┘
```

---

## Cas d'usage principaux

### 1. Masse salariale
- Vue globale brute / nette / retenues par ministère, grade, région
- Évolution mensuelle et annuelle
- Ratio indemnités / salaire de base

### 2. Prédiction
- Projection budgétaire N+1, N+2, N+3 (ARIMA / Prophet)
- Vague de départs en retraite (`date_naissance` + `grade.AGERET`)
- Simulation d'impact : augmentation générale, recrutement, reclassement

### 3. Statistiques
- Pyramide des âges et des grades
- Distribution salariale par genre, ancienneté, région
- Analyse de Pareto des indemnités

### 4. Détection d'anomalies
- **Règles métier** : doublon de paiement, retraité encore payé, net > brut, échelon hors plafond
- **Statistiques** : Z-score, Isolation Forest, loi de Benford
- **Patterns suspects** : même RIB pour plusieurs matricules, saut d'échelon rapide, indemnité incompatible avec le grade
