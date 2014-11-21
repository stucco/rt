#!/bin/sh

echo "Checking out modules..."
mvn -q --non-recursive scm:checkout -Dmodule.name=JSON-java
mvn -q --non-recursive scm:checkout -Dmodule.name=morph
mvn -q --non-recursive scm:checkout -Dmodule.name=html-extractor
mvn -q --non-recursive scm:checkout -Dmodule.name=extractors
mvn -q --non-recursive scm:checkout -Dmodule.name=entity-extractor
mvn -q --non-recursive scm:checkout -Dmodule.name=relation-extractor
mvn -q --non-recursive scm:checkout -Dmodule.name=graph-alignment
mvn -q --non-recursive scm:checkout -Dmodule.name=document-service-client-java

echo "Building rt..."
mvn -q clean install
cd streaming-processor
mvn -q clean package -Dmaven.test.skip=true
cd ..

echo "Cleaning up modules..."
rm -rf JSON-java
rm -rf morph
rm -rf html-extractor
rm -rf extractors
rm -rf entity-extractor
rm -rf relation-extractor
rm -rf graph-alignment
rm -rf document-service-client-java

