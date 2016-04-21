#!/bin/sh

echo "Checking out modules..."
mvn -q --non-recursive scm:checkout -Dmodule.name=rexster-client-java
cd rexster-client-java
sudo mvn -e clean install -Dmaven.test.skip=true
cd ..
mvn --non-recursive scm:checkout -Dmodule.name=entity-extractor
mvn --non-recursive scm:checkout -Dmodule.name=relation-extractor
mvn --non-recursive scm:checkout -Dmodule.name=document-service-client-java
mvn --non-recursive scm:checkout -Dmodule.name=graph-alignment
mvn --non-recursive scm:checkout -Dmodule.name=STIXExtractors

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

