# 📊 ETL WORKSHOP 02

## Project Overview
Build an orchestrated ETL pipeline using Airflow and packaged with Docker. Design a dimensional model for creating a data warehouse (DW) where all data from different sources is loaded after passing through all ETL phases.

---

## Dimensional Model — Star Schema

This star design was created to store the most important information about Spotify songs and all the information about Grammy winners.

The dimensions use surrogate keys to relate to the fact table. These dimensions are the descriptive attributes of each song, artist and grammy. In addition including a date dimension facilitates historical trend analysis using KPIs.

<img width="1158" height="665" alt="Captura de pantalla 2026-04-09 105900" src="https://github.com/user-attachments/assets/3048c0ea-ddb8-4f47-ae3c-a55b2a9a8e18" />

## Grain Definition

> **Each row of the fact table represents a song and its Grammy nomination.**

---

## ETL Logic in Airflow

### Extract
We used a function to extract the data from the CSV dataset using pandas.

We use a function to extract the table grammy from de workshop2 DB and read this tables as a csv.

### Input validation

For the initial validation we use son function that show us:
- Datasets dimensions:
  
```
Spotify Dataset Dimensions: (114000, 21), Grammy DB dimensions: (4810, 10)
```

- Missing values:
```
Missing values in Spotify: 3, Missing values in Grammy: 5403
```
- Duplicated rows:
```
Duplicated rows in Spotify: 920, Duplicated rows in Grammy: 0
```

### Cleaning

For the cleaning we eliminate the duplicated values in the columns:
- Track id and track genre
- Artist, album name, track name and track genre
  
This because somentimes a songs have the duplicated track id but in spotify app corresponds to a diferents music gender and dont have to exist a song with the same artist, album name, track name and gender 

- In the spotify dataset if we have missing values we eliminated that rows.
  
- For the grammys dataset we replace the missing values with unkown
  
- In the first spotify dataset column we change the column name Unamed : 0 to Id
  
- And the last one we change the name of track genre column to gender 

---

### Output validation

For final validation we reuse the code in the input validation to confirm the changes that the cleaning phase did it
- Datasets dimensions:
```
Spotify Dataset Dimensions: (113079, 21), Grammy DB dimensions: (4810, 10) 
```

- Missing values:
```
Missing values in Spotify: 0, Missing values in Grammy: 0
```

- Duplicated rows:
```
Duplicated rows in Spotify: 0, Duplicated rows in Grammy: 0
```

---

### Trasform and Merge

For the merge the columna that we use was artist columna because is one of the columna that are in both dataset. 
- **IMPORTANT:** We did the merge using al left join to maintain the registers, for that reason we have son track that have not grammy information. This merge create a more enriched dataset

For the trasnform we create de dimensions firts and later the fact table, using surrogate keys, based on the star previous star schema for the DW

---

### Load 

In the load phase we define 2 functions:
- A firt one that load all the dimensions and the fact table to a csv files in a folder, this was the local loading.
- A second function that load all the dimensions and the fact table to the DW, in this case its a MySQL DB

---

## Airflow DAG diagram and tasks

To orchestate the ETL pipeline with airflow we define task for each phase:
- Extracting Spotify CSV. 
- Extracting Grammys DB data.
- Input validation
= Cleaning
- Output validation
- Transforming and merging datasets. 
- Loading data into local. 
- Loading data into Data Warehouse.

And this is the diagram

<img width="1213" height="292" alt="Captura de pantalla 2026-04-10 111657" src="https://github.com/user-attachments/assets/da0b3668-0f16-4b20-8c5b-78b99de777f7" />

--- 

## How to run the project

First you have to install Docker Desktop if you dont have it, you can download it from the Microsoft Store in windows

To run this project:
- Download or clone the repository
- Open the folder with VScode
- Open a terminal in VScode and execute this commnads
```
docker compose up -d
```
This commnad go to up the container that contains all the dependeces, requierments and configurations in the yml

- When the container was successfully launched, you must to open a browse and search this ip
```
http://localhost:8081
```

- This is the airflow UI, in the web you must configure a connection:
1. Go to Administration
2. Click on Connections
3. Add new Connection
4. And copy this info in the Connection
    - ID Connection: grammy_db
    - Type Connection: MySQL
    - Host: mysql
    - Login: airflow
    - Password: airflow
    - Port: 3306
    - Schema: workshop2
![WhatsApp Image 2026-04-10 at 1 34 45 AM](https://github.com/user-attachments/assets/8b6b5abe-1abb-43e1-93e3-f85774f43bb3)

---

## How to run the DAG
With the connection and the container correctly now you can run the DAG from the ariflow interface, follow this steps:
1. In ariflow UI go to DAGS
2. Search DAG workshop2
3. Activate the DAG and trigger it

---

## How connect Power BI with the DW from the container
This project consumes the Data Warehouse from Power BI through an ODBC Data Source Name (DSN) configured with the MySQL ODBC driver.

- You must to download the NET and the Unicode Driver
- Open the Data Sources (64-bit) in Windows
- Go to the System DSN
- Click add
- Select MySQL Unicode Driver (9.6 in this case the most recently)
- Configure with this following steps:
  
    - Data source name: Workshop2
    - Port: 3307
    - TCP/IP Server: localhost
    - User: root
    - Password: root
    - Database: workshop2
  
- Click test
- Click OK to save DSN

The repository contains the PowerBI report to run in your PC. Follow the steps below to connect it to your local Data Warehouse after cloning the project.

Locate the file:
- diagrams/workshop2_report.pbix

Open it with Power BI and update Data Source Credentials
- In Power BI go to: Home → Transform Data → Data Source Settings
- Select the ODBC source
- Click Edit Permissions
- Enter your MySQL credentials if prompted
- Confirm that the DSN used is: Workshop2

---

## KPIs and Power Bi report images

<img width="1440" height="806" alt="Captura de pantalla 2026-04-10 114804" src="https://github.com/user-attachments/assets/cdfdf8e9-bd84-48c5-bdc4-4e3fe2024138" />


<img width="1438" height="808" alt="image" src="https://github.com/user-attachments/assets/6f2c8287-dafb-4845-aadc-2869b49467b4" />

