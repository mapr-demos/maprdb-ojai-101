# Simple MapR-DB & OJAI Examples

This project contains examples of OJAI, the JSON API For MapR-DB.


### Pre-requisites

* Java SDK 7 or newer
* Maven 3
* MapR 5.1 Cluster or Sandbox

In your MapR-DB environment change the permissions of the `apps` folder

```
ssh mapr@maprdemo
 
cd /mapr/demo.mapr.com/

chmod 777 apps
```


## Usage

Clone the repository, then

```
mvn clean package
```

and run the application using:

```
mvn exec:java -Dexec.mainClass="com.mapr.db.samples.basic.Ex01SimpleCRUD"
```


## What to look?

Look at the various methods of the `Ex01SimpleCRUD` class:

* createDocuments() that shows different ways of creating documents
* queryDocuments() that shows different ways of querying documents
* updateDocuments() that shows different ways of updating documents


