#!/bin/sh

# checkout all modules
mvn -q --non-recursive scm:checkout -Dmodule.name=JSON-java
mvn -q --non-recursive scm:checkout -Dmodule.name=morph
mvn -q --non-recursive scm:checkout -Dmodule.name=html-extractor
mvn -q --non-recursive scm:checkout -Dmodule.name=extractors
mvn -q --non-recursive scm:checkout -Dmodule.name=entity-extractor
mvn -q --non-recursive scm:checkout -Dmodule.name=relation-extractor
mvn -q --non-recursive scm:checkout -Dmodule.name=graph-alignment
mvn -q --non-recursive scm:checkout -Dmodule.name=document-service-client-java

# build rt
mvn -q clean install -Dmaven.test.skip=true
mvn -q clean package -Dmaven.test.skip=true

# cleanup
rm -rf JSON-java
rm -rf morph
rm -rf html-extractor
rm -rf extractors
rm -rf entity-extractor
rm -rf relation-extractor
rm -rf graph-alignment
rm -rf document-service-client-java