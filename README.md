This is a dashboard website project that performs analysis on e-commerce website logs. The logs are stored on snowflake, data transformations are performed using dbt and pyspark. The dashboard is created using streamlit applications and deployed on aws.


Data Models:

1. Session Analysis:
Calculate the total duration of sessions per user.
Aggregate sales and returned amounts per user.
Identify the most common network protocol used by each user.
2. Customer Segmentation:
Classify customers based on their spending and return behavior.
Create segments like "High Spenders," "Frequent Returners," etc.
3. Sales Performance:
Calculate the total and average sales per country.
Analyze payment methods used by customers.
4. Language Preferences:
Determine the most common languages used by customers.
Group users by language and analyze their spending behavior.