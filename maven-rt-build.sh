#!/bin/sh

#checkout all modules
mvn --non-recursive scm:checkout -Dmodule.name=JSON-java
mvn --non-recursive scm:checkout -Dmodule.name=morph
mvn --non-recursive scm:checkout -Dmodule.name=extractors
mvn --non-recursive scm:checkout -Dmodule.name=entity-extractor
mvn --non-recursive scm:checkout -Dmodule.name=graph-alignment
mvn --non-recursive scm:checkout -Dmodule.name=document-service-client-java
mvn clean install -Dmaven.test.skip=true
rm -rf JSON-java
rm -rf morph
rm -rf extractors
rm -rf entity-extractor
rm -rf graph-alignment
rm -rf document-service-client-java