#!/bin/sh

echo "Checking out modules..."
mvn --non-recursive scm:checkout -Dmodule.name=document-service-client-java
mvn --non-recursive scm:checkout -Dmodule.name=entity-extractor
mvn --non-recursive scm:checkout -Dmodule.name=relation-extractor
mvn --non-recursive scm:checkout -Dmodule.name=STIXExtractors

mvn -q --non-recursive scm:checkout -Dmodule.name=graph-db-connection -DscmVersion=ornl-integration -DscmVersionType=branch
cd graph-db-connection
mvn -e clean install -Dmaven.test.skip=true
cd ..

mvn -q --non-recursive scm:checkout -Dmodule.name=graph-alignment -DscmVersion=loadSTIX -DscmVersionType=branch
cd graph-alignment
mvn -e clean install -Dmaven.test.skip=true
cd ..


echo "Building rt..."
mvn -e clean install -Dmaven.test.skip=true
cd streaming-processor
mvn -e clean package -Dmaven.test.skip=true
cd ..

#echo "Cleaning up modules..."
#rm -rf entity-extractor
#rm -rf relation-extractors
#rm -rf graph-alignment
#rm -rf document-service-client-java
#rm -rf STIXExtractor

