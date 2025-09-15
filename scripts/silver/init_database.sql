/*
===============================================================================
Schema Initialization: Bronze, Silver, Gold
===============================================================================
Purpose:
    These commands create the three core schemas used in the Data Warehouse
    following the Medallion Architecture (Bronze → Silver → Gold).

    - bronze : Stores raw, ingested data in its original form. Acts as a
               staging layer with minimal or no transformations.
    
    - silver : Stores cleaned, standardized, and lightly transformed data.
               This layer ensures consistency and prepares data for modeling.
    
    - gold   : Stores business-ready, curated datasets in the form of
               fact and dimension tables (Star Schema). Used directly by
               analytics, reporting, and BI tools.

Details:
    - IF NOT EXISTS ensures that schemas are only created if they don’t 
      already exist, preventing errors on reruns.
    - These schemas form the foundation for all subsequent tables, views,
      and transformations defined in this project.

Usage:
    Place this script at the beginning of database initialization to
    guarantee the proper schema structure is in place before loading data.
===============================================================================
*/
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
