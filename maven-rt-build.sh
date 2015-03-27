#!/bin/sh

echo "Checking out modules..."
mvn -q --non-recursive scm:checkout -Dmodule.name=JSON-java -Dbranch.name=master
mvn -q --non-recursive scm:checkout -Dmodule.name=morph -Dbranch.name=master
mvn -q --non-recursive scm:checkout -Dmodule.name=html-extractor -Dbranch.name=master
mvn -q --non-recursive scm:checkout -Dmodule.name=extractors -Dbranch.name=master
mvn -q --non-recursive scm:checkout -Dmodule.name=entity-extractor -Dbranch.name=master
mvn -q --non-recursive scm:checkout -Dmodule.name=relation-extractor -Dbranch.name=master
mvn -q --non-recursive scm:checkout -Dmodule.name=graph-alignment -Dbranch.name=schema
mvn -q --non-recursive scm:checkout -Dmodule.name=document-service-client-java -Dbranch.name=master
mvn -q --non-recursive scm:checkout -Dmodule.name=ontology -Dbranch.name=forIndexing

echo "Building rt..."
mvn clean install -Dmaven.test.skip=true
cd streaming-processor
mvn clean package -Dmaven.test.skip=true
cd ..

cp graph-alignment/target/populate_schema.jar streaming-processor/target/.

echo "Cleaning up modules..."
echo "rm -rf JSON-java"
echo "rm -rf morph"
echo "rm -rf html-extractor"
echo "rm -rf extractors"
echo "rm -rf entity-extractor"
echo "rm -rf relation-extractor"
echo "rm -rf graph-alignment"
echo "rm -rf document-service-client-java"
echo "rm -rf ontology"

