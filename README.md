# AQI-prediction-spark-ml
This project is to prove the efficiency of distributed computing and distributed database. The machine learning multiple classification algorithms in spark were used to predict the Air Quality Index in California. 

## Project Description
Air pollution has become a concern of people in California. Prediction of Air Quality Index is, on one hand, benefit by the high frequency, the large number of sampling stations, and the substantial size of relevant data. On the other hand, it is challenging to effectively store, manage, and process a vast amount of data in real-time. 
In this project, we explore a pipeline to store, process and make predictions applying machine learning models to the air quality datasets.
Using a 10-year air quality dataset of California, we develop Logistic Regression and Random Forest classification machine learning models on a local machine as well as on a distributed system. We found that employing Amazon S3, MongoDB and Apache Spark, the distributed setting on a cluster achieved better computational performance than a non-distributed setting.

## Work Flow
The pipeline was built using Apache Spark SQL and Spark machine learning libraries (MLlib) on AWS Elastic MapReduce (EMR).

## Results
