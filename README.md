This is a dashboard website project that performs analysis on e-commerce website logs. The logs are stored on snowflake, data transformations are performed using dbt. The dashboard is created using Tableau. Data orchestration is done on Apache airflow.

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


Tableau dashboard : <div class='tableauPlaceholder' id='viz1725641065605' style='position: relative'><noscript><a href='#'><img alt='Dashboard 1 ' src='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;ec&#47;ecommerce_analysis_17256397926520&#47;Dashboard1&#47;1_rss.png' style='border: none' /></a></noscript><object class='tableauViz'  style='display:none;'><param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' /> <param name='embed_code_version' value='3' /> <param name='site_root' value='' /><param name='name' value='ecommerce_analysis_17256397926520&#47;Dashboard1' /><param name='tabs' value='no' /><param name='toolbar' value='yes' /><param name='static_image' value='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;ec&#47;ecommerce_analysis_17256397926520&#47;Dashboard1&#47;1.png' /> <param name='animate_transition' value='yes' /><param name='display_static_image' value='yes' /><param name='display_spinner' value='yes' /><param name='display_overlay' value='yes' /><param name='display_count' value='yes' /><param name='language' value='en-US' /><param name='filter' value='publish=yes' /></object></div>                <script type='text/javascript'>                    var divElement = document.getElementById('viz1725641065605');                    var vizElement = divElement.getElementsByTagName('object')[0];                    if ( divElement.offsetWidth > 800 ) { vizElement.style.width='1000px';vizElement.style.height='827px';} else if ( divElement.offsetWidth > 500 ) { vizElement.style.width='1000px';vizElement.style.height='827px';} else { vizElement.style.width='100%';vizElement.style.height='1327px';}                     var scriptElement = document.createElement('script');                    scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';                    vizElement.parentNode.insertBefore(scriptElement, vizElement);                </script>

