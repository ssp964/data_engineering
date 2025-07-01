# Fact Data Modelling

## Objective

This directory provides SQL scripts to build robust, analytics-ready fact tables for sports and user activity datasets. The models are designed to:

- Aggregate and deduplicate raw event and performance data
- Track user and player activity over time
- Support efficient storage and querying of time-series and engagement data
- Enable advanced analytics for business intelligence and reporting

## Key Components

- **fact_data_modelling.sql**  
  Uses **game_details.csv** and **games.csv** as data sources. Creates a deduplicated fact table for basketball game details, aggregating player statistics and participation flags for accurate performance analysis.

- **fact_data_modelling_date_list_using_bit.sql**  
  Uses **events.csv** as its data source. Tracks user activity dates and encodes activity history using bit arrays, enabling efficient analysis of user engagement patterns and retention.

- **date_metrics_format_agg.sql**  
  Uses **events.csv** as its data source. Aggregates user metrics (such as site hits) into arrays for each month, supporting advanced analytics on user behavior and monthly trends.

- **HW/**  
  Contains advanced SQL scripts for modeling and analyzing host and device activity, including cumulative and incremental loading, activity date lists, and bitwise activity encoding. Uses **events.csv** and **devices** for all scripts.
  - `host_activity_reduced_incremental_load.sql`: Monthly host activity with hit and unique visitor arrays.
  - `host_activity_datelist_cumulative_query.sql`: Cumulative host activity date lists.
  - `device_activity_datelist_and_datelist_int.sql`: Device activity per user and browser, referencing both events and devices.

## Achievements

Through this project, the following data engineering skills and knowledge were developed and strengthened:

- **Fact Table Design:** Building analytics-ready fact tables for sports and user activity data
- **Deduplication & Aggregation:** Implementing logic to ensure data quality and accurate reporting
- **Time-Series Modeling:** Efficiently storing and querying time-based engagement and performance data
- **Advanced SQL:** Writing SQL for deduplication, aggregation, and bitwise operations
- **User & Player Analytics:** Enabling business intelligence use cases such as retention, engagement, and performance analysis
