This project performs analysis on e-commerce website logs. The logs are stored on snowflake, data transformations are performed using dbt. The dashboard is created using Tableau. Data orchestration is done on Apache airflow.

Tools used:

* Snowflake
* dbt core
* dbt cloud
* Sql
* Apache Airflow
* Python
* Linux
* Tableau
* Pyspark


4 Data Models considered:
    
1. Session Analysis:
Calculate the total duration of sessions per user.
Aggregate sales and returned amounts per user.
Identify the most common network protocol used by each user.
2. retention analysis:
Classify customers based on their spending and return behavior.
Active days mark the customer engagement and retention 
3. Traffic revenue:
Finds the audience that generates the most revenue
4. User cart value:
Determines the average cart value of a user, based on their sessions and activity.


Tableau dashboard : https://public.tableau.com/views/ecommerce_analysis_17256397926520/Dashboard1?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link


![dashboard](<data/Dashboard 1 (2).png>)