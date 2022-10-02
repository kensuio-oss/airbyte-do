Move in folder and prep java version
```
cd airbyte-do
sdk use java 18.0.2.1-tem
```

Build agent
```
mvn clean install && docker build -t kensuio/airbyte-worker -f airbyte-instrumentation/Dockerfile .
```

Move in airbyte repo, which uses our new docker for `workers`
Launch airbyte
```
cd airbyte

docker-compose up
```



Configure job to copy locally the following CSV: https://www.donneesquebec.ca/recherche/fr/dataset/857d007a-f195-434b-bc00-7012a6244a90/resource/16f55019-f05d-4375-a064-b75bce60543d/download/pf-mun-2019-2019.csv




⚠️ RELIES ON 
git@github.com:kensuio/dam-client-java.git

=> BUILD LOCALLY for jackson 2.10

```
git clone git@github.com:kensuio/dam-client-java.git
cd dam-client-java
sdk use java 11.0.16-tem 
JACKSON_VERSION="2.10.0" sbt publishM2
```