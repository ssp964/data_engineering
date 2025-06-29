# Dimensional Data Modelling

## Objective

The goal of this directory is to provide robust, analytics-ready dimensional models for NBA and entertainment datasets. These models are designed to:

- Store and analyze player and actor performance over time
- Track changes in key attributes (e.g., scoring class, activity status)
- Support both cumulative and incremental data loading
- Enable graph-based and SCD Type 2 modeling for advanced analytics

## Key Components

- **Incremental_struct_arrays_models.sql**  
  Uses **player_seasons.csv** as its data source. Utilizes composite types and arrays to efficiently model player season data across multiple seasons.

- **SCD_type2_data_modelling_incremental.sql**  
  Uses **player_seasons.csv** as its data source (via the players table). Implements SCD Type 2 logic to maintain a full history of changes in player scoring class and activity status.

- **graph_data_model.sql**  
  Uses **team.csv**, **games.csv** and **game_details.csv** as its data sources. Represents NBA data as a property graph, supporting network analysis of players, teams, and games.

- **HW.sql**  
  Uses **actor_films.csv** and **actors.csv** as its data source. Demonstrates comprehensive modeling of the actor_films dataset, including cumulative and SCD Type 2 approaches for actors and their filmographies.

## Achievements

Through this project, the following data engineering skills and knowledge were developed and strengthened:

- **Dimensional Data Modeling:** Designing and implementing dimensional models for analytics
- **SCD Type 2 Techniques:** Applying Slowly Changing Dimension logic for historical data tracking
- **Graph Data Modeling:** Modeling complex relationships using property graphs
- **Advanced SQL:** Writing SQL for cumulative, incremental, and backfill data processing
- **Data Pipeline Structuring:** Building scalable pipelines for both sports and entertainment domains
