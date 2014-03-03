#!/bin/sh

#checkout all modules
mvn --non-recursive scm:checkout -Dmodule.name=JSON-java
mvn --non-recursive scm:checkout -Dmodule.name=morph
mvn --non-recursive scm:checkout -Dmodule.name=extractors
mvn --non-recursive scm:checkout -Dmodule.name=entity-extractor
mvn clean install
rm -rf JSON-java
rm -rf morph
rm -rf extractors
rm -rf entity-extractor