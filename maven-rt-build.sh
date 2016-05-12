#!/bin/sh

echo "Checking out modules..."
mvn --non-recursive scm:checkout -Dmodule.name=document-service-client-java -DscmVersion=1.0.1 -DscmVersionType=tag
mvn --non-recursive scm:checkout -Dmodule.name=STIXExtractors -DscmVersion=1.0.0 -DscmVersionType=tag

mvn --non-recursive scm:checkout -Dmodule.name=entity-extractor -DscmVersion=1.0.0 -DscmVersionType=tag
cd entity-extractor
mvn -e clean install -Dmaven.test.skip=true
cd ..

mvn --non-recursive scm:checkout -Dmodule.name=relation-extractor -DscmVersion=1.0.0 -DscmVersionType=tag
cd relation-extractor
mvn -e clean install -Dmaven.test.skip=true
cd ..

mvn -q --non-recursive scm:checkout -Dmodule.name=graph-db-connection -DscmVersion=1.0.0 -DscmVersionType=tag
cd graph-db-connection
mvn -e clean install -Dmaven.test.skip=true
cd ..

mvn -q --non-recursive scm:checkout -Dmodule.name=graph-alignment -DscmVersion=1.0.2 -DscmVersionType=tag
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

