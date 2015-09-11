# usagov-clickstream-analyser
Extract:  
a.	Top ten URLs clicked.  
b.	Top ten URLs per month.  
c.	Top ten URLs per city.  

Input data to this program needs to be downloaded from the below link:

  http://www.usa.gov/About/developer-resources/1usagov.shtml  
  The data is also available in this repository in datasets folder. 

Now clone the project:
```
    git clone https://github.com/mitsy/usagov-clickstream-analyser.git
```      
With this, we are in ~/usagov-clickstream-analyser directory.

To run the Pig script, we need to have Apache Pig installed on our machine and run the below command:

```
    pig -x local clickstream.pig
```
This will create an **output** directory inside usagov-clickstream-analyser/ which will have **top10percity**, **top10permonth** and **top10urls** folders. Check the part files inside these folders.

To run the Spark program, we need to have Apache Spark installed on our machine and run the below command:

```
    pyspark clickstream_spark.py
```
This will create a **sparkout** directory inside usagov-clickstream-analyser/ which will have **top10cities**, **top10months** and **top10urls** folders. Check the part files inside these folders.


