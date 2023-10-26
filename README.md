# peel-region-real-estate-dashboard

# Motivation
Recently, I have been interested in the real-estate market. Many of the websites I use to look at listings provide details about the home itself, but don’t necessarily compare the list price against other homes available on the market. I wanted to create a dashboard where I can select a listing, and compare the list price against similar homes that are available. 

You can find the dashboard at this [link](https://dash-app-ctyqyt7wvq-uc.a.run.app/).

# Technologies Used
Different technologies were used to complete this project. Below is a list of technologies and products that were used. 

**Programming Languages**: Python, SQL, CSS  
**Data Processing**: PySpark  
**Data Visualization**: Plotly, Dash  
**Relational Database**: PostgreSQL  
**Cloud**: GCP - Cloud Storage, Cloud SQL, Dataproc, Compute Engine, Cloud Run  
**Data Generator**: Synth  
**Orchestration**: Airflow  
**Other**: Terraform, Docker  

# About the Data 
Ideally, I would like to use data from real listings from a multiple listing service. However, to get access to this data in Canada, you either need to be a member of a real estate board, or pay for an API that provides access. I am neither a real-estate agent, nor did I want to pay for an API while I build out a proof-of-concept. 

My solution was to use a data generator. The generator of choice was Synth, which is an open-source tool that provides a command line tool to generate data. Synth takes in a schema, which defines the column names and data types for each field, and generates random data accordingly. You can read more about Synth [here](https://www.getsynth.com/docs/getting_started/synth). 

**I do want take the time and explicitly say that the listings in the app are completely fake and do not represent real-world addresses.** 

# Data Pipeline 
The first step in the data pipeline was to generate the listings. The generated listings were saved to JSON files locally before they were uploaded to cloud storage. Next, transformations were applied to the raw listings to add details like the age and square footage range of the listing. It was at this stage where I also created a field for the list price, using a custom function that created realistic (or at least more realistic than a completely random number) list prices based on the size of the home and property type. These transformations were completed using PySpark, and the transformed data was loaded into a Postgres database. I wanted to simulate listings being updated or removed from the database, so I created two additional PySpark jobs which would update and delete a small random percentage of listings from the database. 

Two additional tasks are included in the pipeline, which involve the Spark cluster creation and deletion. To keep costs low, the Spark cluster is created when the DAG is triggered, and is torn down once all the Spark related tasks are complete. 

The data pipeline was orchestrated using Airflow, and it runs four times a day.  

# Provisioning Cloud Resources
A number of cloud resources were used for this project. I chose to go with GCP as they provide a $300 credit for new users. Terraform was used to provision a Cloud Storage bucket, and a Cloud SQL Postgres instance. I initially set up Airflow locally, but to ensure my pipeline was running when my computer went to sleep or powered down, I later provisioned and installed Airflow on a Compute Engine instance. As mentioned earlier, PySpark was used to process the raw listings, and a Dataproc cluster was provisioned and torn down after the PySpark jobs were completed as part of the data pipeline. 

Other GCP products used include Cloud Run to deploy the app, Artifact Registry to store the container image for the app, and Secrets Manager to prevent credentials being stored as plaintext in the source code. 

# Dashboard and Web App
The dashboard was built using Plotly and Dash. Data is pulled from the Cloud SQL database to populate the fields in the dashboard. Users can either search for or select a listing by address from the dropdown menu and the dashboard will display its listing details. 

## What is ‘Similar’? 
For the purpose of this dashboard, a listing that is similar to the selected listing should be in the same city, and the same property type (semi-detached, townhome or single detached). The two bar graphs at the bottom of the dashboard compares the selected listing against similar homes in the database.

The bar graph on the left compares the list price of the selected listing against the average price of other listings in the same city grouped by the Square Foot Range. Similarly, the bar graph on the right compares the list price of the selected listing against the average price of other listings in the same city grouped by Age Range. 

Using these bar graphs, users can get an indication of whether a listing is priced fairly when compared to similar homes on the market. 
