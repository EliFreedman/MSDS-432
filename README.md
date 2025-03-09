# MSDS-432: Foundations of Data Engineering

This course introduces data engineering concepts and technologies. It reviews design principles, development processes, and management of information systems with a focus on containerized microservices and cloud native applications. The course reviews data exchange formats, concurrency control of interacting processes, data communication protocols, standards for designing application programming interfaces, distributed processing, and information systems architecture. Students learn about the automated deployment and scaling of batch, interactive, and streaming data pipelines. They learn how to design, implement, and maintain data-intensive applications in cloud and on-premises environments. This is a programming-intensive course that includes a full-stack development project.

# Final Project - Chicago Business Intelligence for Strategic Planning

In this project, you have been tasked as a full-stack developer to build an application that will be used by data scientists and business analysts for exploratory data analysis and to create different business intelligence reports for the city of Chicago; these reports will be utilized in the strategic planning and the industrial and neighborhood infrastructure investments. The City of Chicago publishes and updates its datasets on its data portal server (https://data.cityofchicago.org/ ) in 16 categories. The 3 categories that this project will utilize for exploratory data analysis and creating the business intelligence reports are: Transportation, Buildings, and Health & Human Services.

### Requirement 1

The business intelligence reports are geared toward tracking and forecasting events that have direct or indirect negative or positive impacts on businesses and neighborhoods in different zip codes within the city of Chicago. The business intelligence reports will be used to send alerts to taxi drivers about the state of COVID-19 in the different zip codes in order to avoid taxi drivers to be the super spreaders in the different zip codes and neighborhoods. For this report, the taxi trips and daily COVID19 datasets for the city of Chicago will be used.

The City of Chicago is also interested to forecast COVID-19 alerts (Low, Medium, High) on daily/weekly basis to the residents of the different neighborhoods considering the counts of the taxi trips and COVID-19 positive test cases.

### Requirement 2

There are two major airports within the city of Chicago: O’Hare and Midway. And the City of Chicago is interested to track trips from these airports to the different zip codes and the reported COVID-19 positive test cases. The city of Chicago is interested to monitor the traffic of the taxi trips from these airports to the different neighborhoods and zip codes.

### Requirement 3

The city of Chicago has created the COVID-19 Community Vulnerability Index (CCVI) to identify communities that have been disproportionately affected by COVID-19 and are vulnerable to barriers to COVID-19 vaccine uptake. The city of Chicago is interested to track the number of taxi trips from/to the neighborhoods that have CCVI Category with value HIGH

### Requirement 4

For streetscaping investment and planning, the city of Chicago is interested to forecast daily, weekly, and monthly traffic patterns utilizing the taxi trips for the different zip codes.

### Requirement 5

For industrial and neighborhood infrastructure investment, the city of Chicago is interested to invest in top 5 neighborhoods with highest unemployment rate and poverty rate and waive the fees for building permits in those neighborhoods in order to encourage businesses to develop and invest in those neighborhoods. Both, building permits and unemployment, datasets will be used in this report.

### Requirement 6

According to a report published by Crain’s Chicago Business, The “little guys”, small businesses, have trouble competing with the big players like Amazon and Walmart for warehouse spaces. To help small business, assume a new imaginary program has been piloted with the name Illinois Small Business Emergency Loan Fund Delta to offer small businesses low interest loans of up to $250,000 for those applicants with PERMIT_TYPE of PERMIT - NEW CONSTRUCTION in the zip code that has the lowest number of PERMIT - NEW CONSTRUCTION applications and PER CAPITA INCOME is less than 30,000 for the planned construction site. Both, building permits and unemployment, datasets will be used in this report.

# Overview

The directories listed below are each their own containerized microservice. Within each service folder is a `cmd` and `internal` folder. The `cmd` subfolder contains the main file that executes the logic defined within the `interal` subfolder. The `internal` subfolder contains two folders: the functions unique to each microservice that define the unique logic to process data and the `queue` folder which defines how the microservice will fetch and publish data from the queueing service.

### File Structure:
```
├───cleaner-service
│   ├───cmd
│   │   └───cleaner
│   └───internal
│       ├───clean
│       └───queue
├───fetcher-service
│   ├───cmd
│   │   └───fetcher
│   └───internal
│       ├───fetch
│       └───queue
├───storage-service
│   ├───cmd
│   │   └───storage
│   └───internal
│       ├───db
│       └───queue
└───transformer-service
    ├───cmd
    │   └───transformer
    └───internal
        ├───queue
        └───transform
```

## Fetcher

`fetcher-service` is the first processing microservice to execute. Within the `main.go` file is defined the URLs to fetch data from as well as the amount of data to fetch in each iteration of the loop. The speed at which data is pulled from the URLs is purposely throttled due to the limitations of Google's Maps API which is utilized in a later stage. For each URL, a goroutine is initialized and fetches data. If the reponse code is okay and the body of the response is readable, the data is unmarshalled. Then, the name of the source the data was fetched from is added to the structure as `table_name`, remarshalled, and then published to a RabbitMQ queue called `<table_name>_raw`. This process is repeated until the amount of fetched data that is predefined is reached.

## Cleaner

`cleaner-service` consumes data from each raw data queue that was published via the `fetcher-service` and processes the message. For each source, there is a unique handler function that converts each value into the correct data type and drops rows that do not contain sufficient information for further processing. There are also types for each source that continue to be used throughout the remainder of the process. After each message is processed, a logging message is printed which contains the cleaned data structure followed by the number of records that were dropped and why. Then, the clean data structure is published as a new queue called `<table_name>_bronze` to RabbitMQ.

## Transformer

`transformer-service` consumes data from each bronze data queue that was published by the `cleaner-service`. It enriches datasets that contain location-based information by utilizing the Google Maps API to coalesce latitiudes and longitudes into addresses and zip codes. Similar to `cleaner-service`, any rows that cannot be properly converted are dropped and logged. This transformed data structure is published as a new queue called `<table_name>_silver` to RabbitMQ.

## Storage

`storage-service` consumes data from each silver data queue that was published by the `transformer-service`. It first connects to the Postgres instance and then begins to read in the data from the queue. As data is read in, it first checks if there is a corresponding table that exists in the database to store the data in. If no such table exists, one is generated based on the schema of the message. Then, records are inserted into the database based on the queue that they are processed from with logs printed for successful and unsuccessful insertions. After all data is ingested, the connection is closed.

## Other Services

To support the previous microservices, both a Postgres and RabbitMQ microservice are required. The RabbitMQ service maintains messages sent between other services and is used as a broker for communication. The Postgres service stores the completely processed data for further use.

# Getting Started

In order to run the aforementioned microservices, you need to have Docker Desktop (https://www.docker.com/products/docker-desktop/) installed and running. You also must have Postgres installed. The configurations defined in the sample YAML file utilize version 14, so if you are using a different version make sure to change that config. Once Docker Desktop is running, navigate to the `src` directory and run `docker-compose up -d` to initiate the services. The microservices will launch in the proper order as specified in the YAML file, and you are good to go!