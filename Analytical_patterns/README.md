# User Engagement Analytics & Behavioral Modeling Framework

## Background 

**DataExpert.io** is an online education platform founded by **Zach Wilson** in **April 2023** that focuses on teaching modern data engineering skills through real-world case studies and hands-on projects. 

Operating in the **edtech industry**, the platform uses a **subscription-based model** to offer tutorials and interactive exercises that simulate production-grade data pipelines.

---

## Analytical Focus Areas and Behavioral Modeling Patterns

### User Lifecycle Engagement
Tracks user activity trends, classifies lifecycle stages, analyzes inactivity/reactivation patterns, and measures overall engagement duration.

---

## Data Source & Structure

The dataset used for engagement analytics is derived from the **events table**, which captures user interaction logs from the DataExpert.io learning platform. The dataset contains **16,830 rows** of user interaction data. Each row represents an event performed by a user and contains the following key fields:

| Column Name  | Data Type | Description |
|--------------|-----------|-------------|
| `url`        | text      | URL path the user accessed on the platform |
| `referrer`   | text      | Source URL that referred the user to the accessed page |
| `user_id`    | numeric   | Unique identifier representing the user |
| `device_id`  | numeric   | Fingerprint representing the user's device/browser |
| `host`       | text      | Domain or subdomain used (e.g., learn.dataexpert.io, admin.dataexpert.io) |
| `event_time` | text      | Timestamp string capturing when the interaction occurred (needs conversion) |

---

## Overview of Focus Areas and Modelling Patterns

### User Lifecycle Engagement

This modeling pattern tracks user engagement over time by assigning each user to a lifecycle state on a **daily and weekly basis** based on activity. It enables structured classification into behavioral segments: 

- **New**
- **Retained** 
- **Resurrected**
- **Churned**
- **Stale**

These classifications are based on presence or absence of event data. These states form the foundation for engagement tracking, reactivation strategies, and retention measurement.

This pattern is implemented via a **cumulative update query** against the `users_growth_accounting` table which will be used by analysts, derived from the `events` table. Each lifecycle label is generated using deterministic rules around timestamped activity.

> **Note:** The SQL query used for this focus area can be found here: [users_growth_accounting.sql](https://github.com/ssp964/data_engineering/blob/main/Analytical_patterns/users_growth_accounting.sql)

