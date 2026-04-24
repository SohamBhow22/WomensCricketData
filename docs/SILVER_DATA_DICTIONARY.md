# SILVER_DATA_DICTIONARY.md

# Silver Layer Data Dictionary

## Overview

The Silver layer is the curated transformation layer between Bronze raw ingestion and Gold business-facing analytics marts.

It converts raw source cricket data into clean, standardized, conformed dimensional and fact models suitable for:

- analytics
- dashboards
- reporting
- downstream Gold marts
- historical trend analysis
- data science / feature engineering

The Silver layer is where raw nested cricket files become usable warehouse data.

---

## Layer Objectives

- flatten nested source JSON structures
- standardize inconsistent raw values
- create reusable conformed dimensions
- create multi-grain fact tables
- support historical tracking where applicable
- improve trust and data quality
- create auditable warehouse-ready structures
- simplify Gold layer creation

---

## Design Principles

### Conformed Dimensions

Shared dimensions are reused across multiple fact tables.

Examples:

- DIM_PLAYER
- DIM_TEAM
- DIM_VENUE
- DIM_EVENT

### Multi-Grain Facts

Different analytical grains are intentionally supported:

- Ball level
- Innings level
- Match level
- Player-match level

### Historical Readiness

Where appropriate, dimensions are designed to support SCD evolution.

### Trust Before Beauty

Silver prioritizes correctness over presentation.

---

## Table Inventory

### Dimensions

- DIM_MATCH
- DIM_PLAYER
- DIM_PLAYER_ALIAS
- DIM_TEAM
- DIM_VENUE
- DIM_EVENT

### Facts

- FACT_BALL_BY_BALL
- FACT_PLAYER_MATCH_STATS
- FACT_INNINGS_SUMMARY
- FACT_MATCH_SUMMARY
- FACT_LOAD_CONTROL

---

# Dimensions

## DIM_MATCH

### Grain

One row per match.

### Purpose

Stores standardized match metadata.

### Key Columns

| Column | Meaning |
|---|---|
| match_id | Unique match identifier |
| match_date | Match date |
| season | Season label |
| competition_name | Raw competition name |
| match_type | T20 / ODI / Test etc. |
| team1 | First listed team |
| team2 | Second listed team |
| winner | Winning team |
| toss_winner | Toss winner |
| toss_decision | Toss decision |
| venue_name | Venue |
| city | Match city |
| event_sk | Foreign key to DIM_EVENT |
| created_ts | Audit timestamp |

### Typical Uses

- tournament filtering
- schedule analytics
- metadata joins

---

## DIM_PLAYER

### Grain

One row per player version.

### Purpose

Canonical player master dimension with SCD-ready design.

### Key Columns

| Column | Meaning |
|---|---|
| player_sk | Surrogate key |
| player_bk | Stable business key |
| player_name | Canonical player name |
| player_display_name | Preferred display label |
| short_name | Short form |
| effective_from | Version start |
| effective_to | Version end |
| is_current | Current version flag |
| created_ts | Audit timestamp |
| updated_ts | Audit timestamp |

### Typical Uses

- player joins
- career aggregation
- historical identity tracking

---

## DIM_PLAYER_ALIAS

### Grain

One row per alias mapping.

### Purpose

Maps alternate source player names to canonical identity.

### Key Columns

| Column | Meaning |
|---|---|
| alias_sk | Surrogate key |
| alias_name | Alternate source name |
| alias_name_normalized | Standardized alias |
| player_bk | Stable identity |
| canonical_player_name | Canonical player |
| match_method | Mapping method |
| confidence_score | Match confidence |
| is_current | Active flag |

### Typical Uses

- source name resolution
- improved joins

---

## DIM_TEAM

### Grain

One row per team.

### Purpose

Standard team master.

| Column | Meaning |
|---|---|
| team_sk | Surrogate key |
| team_name | Team name |
| created_ts | Audit timestamp |

---

## DIM_VENUE

### Grain

One row per venue.

### Purpose

Venue master dimension.

| Column | Meaning |
|---|---|
| venue_sk | Surrogate key |
| venue_name | Ground name |
| city | Venue city |
| created_ts | Audit timestamp |

---

## DIM_EVENT

### Grain

One row per standardized competition / event.

### Purpose

Standardizes tournaments, leagues, and series.

| Column | Meaning |
|---|---|
| event_sk | Surrogate key |
| competition_name_raw | Raw source event name |
| competition_name_std | Standardized event name |
| event_type | League / World Event / Series / Other |
| match_format | T20 / ODI / Test |
| gender | Competition gender |
| is_international | 1 if international |
| franchise_flag | 1 if franchise |
| season_first_seen | Earliest observed season |
| season_last_seen | Latest observed season |

### Typical Uses

- tournament analytics
- domestic vs international segmentation

---

# Facts

## FACT_BALL_BY_BALL

### Grain

One row per delivery.

### Purpose

Atomic cricket event table.

| Column | Meaning |
|---|---|
| ball_id | Unique ball identifier |
| match_id | Match id |
| innings_no | Innings number |
| over_no | Source over number |
| nth_over | Human over number |
| ball_no | Ball within over |
| striker | Batter on strike |
| non_striker | Batter at non-striker end |
| bowler | Bowler |
| striker_player_sk | FK to player |
| non_striker_player_sk | FK to player |
| bowler_player_sk | FK to player |
| batting_team_sk | FK to team |
| total_runs | Total runs |
| runs_batter | Batter runs |
| extras | Extra runs |
| wicket_flag | 1 if wicket |
| phase | Powerplay / Middle / Death |

### Typical Uses

- advanced player analytics
- phase scoring
- bowling pressure analysis

---

## FACT_PLAYER_MATCH_STATS

### Grain

One row per player per match.

### Purpose

Pre-aggregated player performance table.

| Column | Meaning |
|---|---|
| match_id | Match id |
| player_sk | Player key |
| runs_scored | Runs scored |
| balls_faced | Balls faced |
| fours | Fours hit |
| sixes | Sixes hit |
| wickets | Wickets taken |
| balls_bowled | Legal balls bowled |

---

## FACT_INNINGS_SUMMARY

### Grain

One row per innings.

### Purpose

Middle grain between ball and match.

| Column | Meaning |
|---|---|
| innings_id | Unique innings key |
| match_id | Match id |
| innings_no | Innings number |
| batting_team_sk | Team key |
| runs_scored | Innings runs |
| wickets_lost | Wickets lost |
| legal_balls | Valid balls faced |
| overs_batted | Overs consumed |
| run_rate | Runs per over |
| fours | Boundary fours |
| sixes | Boundary sixes |
| extras | Extras |
| dot_balls | Dot balls |
| boundaries | Total boundaries |

---

## FACT_MATCH_SUMMARY

### Grain

One row per team innings / match summary row.

### Purpose

Match-level summarized performance.

| Column | Meaning |
|---|---|
| match_id | Match id |
| batting_team_sk | Team key |
| venue_sk | Venue key |
| total_runs | Team total |
| wickets_lost | Wickets lost |
| overs_batted | Overs faced |
| run_rate | Run rate |

---

## FACT_LOAD_CONTROL

### Grain

One row per load execution event.

### Purpose

Operational monitoring and pipeline auditability.

| Column | Meaning |
|---|---|
| load_id | Load id |
| batch_id | Pipeline batch id |
| layer_name | Bronze / Silver / Gold / Pipeline |
| source_file | File processed |
| load_status | SUCCESS / FAILED |
| started_ts | Start time |
| completed_ts | End time |
| error_message | Failure reason |

---

## Relationships

- DIM_MATCH.event_sk -> DIM_EVENT.event_sk
- FACT_BALL_BY_BALL.match_id -> DIM_MATCH.match_id
- FACT_BALL_BY_BALL.striker_player_sk -> DIM_PLAYER.player_sk
- FACT_BALL_BY_BALL.non_striker_player_sk -> DIM_PLAYER.player_sk
- FACT_BALL_BY_BALL.bowler_player_sk -> DIM_PLAYER.player_sk
- FACT_BALL_BY_BALL.batting_team_sk -> DIM_TEAM.team_sk
- FACT_PLAYER_MATCH_STATS.player_sk -> DIM_PLAYER.player_sk
- FACT_INNINGS_SUMMARY.match_id -> DIM_MATCH.match_id
- FACT_INNINGS_SUMMARY.batting_team_sk -> DIM_TEAM.team_sk
- FACT_MATCH_SUMMARY.match_id -> DIM_MATCH.match_id
- FACT_MATCH_SUMMARY.batting_team_sk -> DIM_TEAM.team_sk
- FACT_MATCH_SUMMARY.venue_sk -> DIM_VENUE.venue_sk

---

## Validation / Trust Checks

Recommended recurring checks:

- duplicate ball_id
- duplicate innings_id
- duplicate match_id
- null foreign keys
- innings wickets > 10
- negative runs
- totals reconciliation between facts
- unresolved player identities
- failed pipeline loads

---

## Known Current Limitations

- Some player keys may remain unresolved due to incomplete people source data
- Alias mapping intentionally conservative
- More historical source files may still be added
- Large-scale performance tuning can be added later
- SCD merge automation can be improved later

---

## Proposed Enhancements (Backlog)

### Identity & Mastering

- stewarded unresolved player workflow
- advanced alias matching
- cross-source player mastering

### SCD Enhancements

- automated Type 2 merge framework
- team rename history
- venue rename history

### Data Quality

- automated scorecards
- anomaly alerts
- duplicate prevention framework

### Performance

- incremental Silver refreshes
- partition-aware processing
- large-volume optimization

### Metadata

- row counts by run
- lineage outputs
- refresh audit dashboards

---

## Typical Analytics Use Cases

- batter career statistics
- bowling economy trends
- venue scoring trends
- tournament comparisons
- chase vs defend win rates
- innings phase scoring
- player impact metrics
- team form analysis

---

## Silver Exit Criteria

Silver layer is considered production-ready when:

- pipeline runs cleanly
- validation checks are acceptable
- schemas are stable
- known issues are documented
- Gold marts can be built confidently

---

## Summary

The Silver layer is the trusted warehouse foundation that transforms raw cricket data into reusable analytical assets powering the Gold layer.