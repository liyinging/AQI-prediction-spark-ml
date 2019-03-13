# AQI-prediction-spark-ml
This project is to prove the efficiency of distributed computing and distributed database. The machine learning multiple classification algorithms in spark were used to predict the Air Quality Index in California. 

## Project Description
Air pollution has become a concern of people in California. Prediction of Air Quality Index is, on one hand, benefit by the high frequency, the large number of sampling stations, and the substantial size of relevant data. On the other hand, it is challenging to effectively store, manage, and process a vast amount of data in real-time. 
In this project, we explore a pipeline to store, process and make predictions applying machine learning models to the air quality datasets.
Using a 10-year air quality dataset of California, we develop Logistic Regression and Random Forest classification machine learning models on a local machine as well as on a distributed system. We found that employing Amazon S3, MongoDB and Apache Spark, the distributed setting on a cluster achieved better computational performance than a non-distributed setting.

## Work Flow
The pipeline was built using Apache Spark SQL and Spark machine learning libraries (MLlib) on AWS Elastic MapReduce (EMR).
<img width="1103" alt="system_workflow" src="https://user-images.githubusercontent.com/40928821/54246024-6a6ed900-44f0-11e9-8ec6-9a605c2c2c59.png">

## Results
![result2](https://user-images.githubusercontent.com/40928821/54239217-cbd67e00-44d7-11e9-8e4b-da35a34d3ba9.png)

## Conclusion
Deploying the model on a cluster with various hyperparameter settings proved that a distributed setting on a cluster achieved better computational performance than a non-distributed setting. Compared to nondistributed settings, the study result is promising, proving that the designed pipeline can provide a scalable and efficient throughput of machine learning algorithms for air quality prediction.
