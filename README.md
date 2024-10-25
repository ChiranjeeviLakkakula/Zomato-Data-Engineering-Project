# _Zomato Data Engineering Project - Data Extraction and Analysis_

## _Introduction_
1. In this project I have built an ETL(Extract, Transform, Load) pipeline using the raw JSON files. The pipeline will extract data from raw JSON files, transform it into desired format and load into AWS data store.
2. Analyzed the dataset containing information about restaurants worldwide. By conducting a thorough data analysis, we sought to understand the relationship between various factors, such as restaurant location, price, online ordering availability, and customer ratings.

## _Architecture - End to End ETL Pipeline_
![Architecture Diagram](https://github.com/ChiranjeeviLakkakula/Zomato-Data-Engineering-Project/blob/main/Zomato-ETL-Pipeline.png)

## _About Dataset_
Zomato API Analysis is one of the most useful analysis for foodies who want to taste the best cuisines of every part of the world which lies in their budget. This analysis is also for those who want to find the value for money restaurants in various parts of the country for the cuisines. Additionally, this analysis caters the needs of people who are striving to get the best cuisine of the country and which locality of that country serves that cuisines with maximum number of restaurants.♨️

API documentation - [Zomato API](https://developers.zomato.com/api#headline1)

## _Language Used_

**Python** 
1. Used python to extract data from Spotify API into JSON format.
2. Converted the extracted data set into Dataframe and performed transformation

## _Services Used_

1. **AWS S3:** Amazon S3 (Simple Storage Service) is a cloud-based object storage service provided by Amazon Web Services (AWS). It offers scalability, data availability, security, and performance to store and manage any amount of data for various use cases like data lakes, websites, mobile applications, and big data analytics

2. **AWS Lambda:** AWS Lambda is a serverless computing service provided by Amazon Web Services (AWS). It allows developers to run code without the need to provision or manage servers. Lambda functions are executed in response to events, and the service automatically handles the computing resources, scaling, and administration

3. **AWS Cloud Watch:** Amazon CloudWatch is a monitoring and observability service provided by AWS. It collects and tracks metrics, collects and monitors log files, sets alarms, and automatically reacts to changes in your AWS resources. CloudWatch provides real-time insights into resource and application performance, helping you maintain operational health and optimize resource use

4. **AWS Glue Crawler:** An AWS Glue Crawler is a feature of AWS Glue that automatically discovers and catalogs metadata from your data sources. It scans your data, identifies the structure (like table names, columns, and data types), and updates the AWS Glue Data Catalog. This makes it easier to analyze your data without manually defining the schema for each dataset.

5. **Data Catalog:** You what is Data Catalog Copilot A Data Catalog is a centralized repository that organizes and manages metadata, making it easier for data users to find and understand data assets within an organization. It provides detailed information about the data's structure, location, ownership, usage, and relationships with other data assets.

6. **Amazon Athena:** Amazon Athena is a serverless, interactive query service provided by AWS that allows you to analyze data directly in Amazon S3 using standard SQL. It's designed to be easy to use, requiring no infrastructure to manage, and you only pay for the queries you run. Athena can process large-scale data, making it ideal for log analysis, data analytics, and running interactive queries on petabyte-scale datasets.

## _Python Packages Used_
````
pip install pandas
pip install spotipy
````
## _Data Pipeline flow_

Extract Data from Zomato API(Raw JSON files)-> Store raw data -> Transformation function -> Transform data and load -> Query data using Athena
