# ACBP × Calcite — Minimal DSL → SQL Tests

This tiny repo demonstrates the **requirements** and **string-in → SQL-out** tests shared with the Calcite dev list.  
It parses a minimal **ACBP DSL** and emits deterministic SQL `SELECT … CASE … END AS action_id` for multiple dialects.

> First cut: a lightweight generator to make the tests runnable today.  
> Next step: swap in **Calcite RelBuilder → RelToSql → SqlDialect** while keeping the exact same golden outputs.

---

## Prerequisites

- **Java 17+**
- **Maven 3.9+**
- Bash/PowerShell/CMD (any shell is fine)

Check versions:
```bash
java -version
mvn -version
```

Quick Start
```bash
# from the repo root
mvn -q -DskipTests=false test
Expected: 4 tests pass.
```

## What’s Implemented (v0.1)

* A tiny ACBP DSL for the examples used on the mailing list:
- from, time_column
- category name := enum('A','B',...) (compile-time; used by ACBP - proofs; not required to render SQL unless referenced)
- category name := enum(select code from ref_...) (table-driven categories)
- ref name := "table_name"
- flag name := <predicate> (boolean expression over columns/categories/ref tables)
- decision action_id { when <cond> -> <int> ... else -> <int> } (ordered, first-match-wins)
* A minimal generator that:
- Expands flag names inside when conditions into their predicates.
- Prints a portable CASE expression.
- Renders time window idioms per dialect:
- - PostgreSQL: now() - interval 'N days'
- - BigQuery: TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL N DAY)
- - ClickHouse: now() - INTERVAL N DAY
* Deterministic output: same inputs → same SQL string (modulo whitespace).

## Test Cases Included

The tests mirror the email examples exactly:

1) Test A — PostgreSQL (inline categories + flags)
Admission + emergency → action 2; else 1.

2) Test B — BigQuery (inline categories + ref-table membership flag)
Abnormal ORU + critical LOINC → 3; abnormal ORU → 2; else 1.

3) Test C — ClickHouse (inline categories + STAT)
STAT order + abnormal result → 3; STAT order → 2; else 1.

4) Test D — PostgreSQL (table-driven categories + flag)
Categories and “emergency classes” sourced from reference tables.

All four tests compare normalized SQL (collapse whitespace + lowercase) to avoid superficial diffs.

Run:

```bash
mvn -q -DskipTests=false test
```

## Repository Layout

acbp-calcite-tests/
├─ README.md
├─ pom.xml
├─ .gitignore
├─ .editorconfig
├─ src/
│  ├─ main/java/org/acbp/
│  │  ├─ Dialect.java          # Dialect enum (POSTGRESQL, BIGQUERY, CLICKHOUSE)
│  │  ├─ AcbpModel.java        # Minimal DSL parser for this demo
│  │  └─ AcbpToSql.java        # Deterministic DSL → SQL generator (swappable to Calcite)
│  └─ test/java/org/acbp/
│     └─ AcbpToSqlTests.java   # Four golden tests (A–D)

## How to Read/Extend the DSL

* Categories: finite enums; used by ACBP for coverage/soundness/dedup proofs. They only influence SQL when referenced in predicates.
* Flags: named boolean predicates; used inside decision clauses.
* Decision: ordered when … -> N rules with a required else -> N.

Example (PostgreSQL case from Test A):

```txt

model hl7_v1 {
  from hl7_messages
  time_column event_ts

  category message_type  := enum('ADT','ORM','ORU')
  category trigger_event := enum('A01','A04','A03','O01','R01')
  category patient_class := enum('E','I','O')

  flag is_admission := message_type = 'ADT' and trigger_event in ('A01','A04')
  flag is_emergency := patient_class = 'E'

  decision action_id {
    when is_admission and is_emergency -> 2
    else -> 1
  }
}

```

## Swapping in Calcite (next step)

Keep the tests/golden strings unchanged. Replace AcbpToSql internals with:
1) Build a RelNode via RelBuilder (scan → project → CASE).
2) Convert to SqlNode using RelToSqlConverter.
3) Unparse with target SqlDialect + SqlPrettyWriter.
4) Leave DDL (CTAS/REPLACE) to external templates; tests cover the SELECT only.

Notes:
* For reviewers who prefer AST-level checks, parse expected and actual strings to SqlNode and compare trees (the test harness has a simple string normalizer now; tree-compare can be added alongside).

## Assumptions to Confirm

* Time window is an option (windowDays) mapped per dialect; no UDFs required.
* No joins in minimal tests (staging joins happen upstream). Ref-table membership expressed via IN (SELECT …) / EXISTS.
* NULL handling is explicit (IS NULL / IS NOT NULL) when needed; minimal tests avoid NULL semantics.

Commands
```bash
# run tests
mvn -q -DskipTests=false test

# verbose test output
mvn test -DtrimStackTrace=false
```

## License
DotK Proprietary Noncommercial License v1.0
SPDX-License-Identifier: LicenseRef-DotK-Proprietary-NC-1.0