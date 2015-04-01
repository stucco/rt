#!/bin/sh

echo "Checking out modules..."
mvn --non-recursive scm:checkout -Dmodule.name=JSON-java
mvn --non-recursive scm:checkout -Dmodule.name=morph
mvn --non-recursive scm:checkout -Dmodule.name=html-extractor
mvn --non-recursive scm:checkout -Dmodule.name=extractors
mvn --non-recursive scm:checkout -Dmodule.name=entity-extractor
mvn --non-recursive scm:checkout -Dmodule.name=relation-extractor
mvn --non-recursive scm:checkout -Dmodule.name=document-service-client-java
mvn --non-recursive scm:checkout -Dmodule.name=rexster-client-java
mvn --non-recursive scm:checkout -Dmodule.name=graph-alignment -DscmVersion=development -DscmVersionType=branch

echo "Building rt..."
mvn -e clean install -Dmaven.test.skip=true
cd streaming-processor
mvn -e clean package -Dmaven.test.skip=true
cd ..

#echo "Cleaning up modules..."
#rm -rf JSON-java
#rm -rf morph
#rm -rf html-extractor
#rm -rf extractors
#rm -rf entity-extractor
#rm -rf relation-extractor
#rm -rf graph-alignment
#rm -rf document-service-client-java

