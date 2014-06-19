# rt

## Build Status

master: [![Build Status](https://travis-ci.org/stucco/rt.png?branch=master)]
(https://travis-ci.org/stucco/rt)
dev: [![Build Status](https://travis-ci.org/stucco/rt.png?branch=dev)]
(https://travis-ci.org/stucco/rt)
de-storm: [![Build Status](https://travis-ci.org/stucco/rt.png?branch=de-storm)]
(https://travis-ci.org/stucco/rt)

## Running Topology

### Prerequisites
* RabbitMQ running on the default port 5672
* [document-service](https://github.com/stucco/document-service) running on the default port 8118
* Rexster/Titan/query-service running
* [Supervisord](http://supervisord.org/introduction.html) installed via:

		pip install supervisor --pre
		
	or
		
		easy_install supervisor

### Compile and Execute Locally
1. Open a terminal and cd to the rt directory
2. Run the following commands:
	
		./maven-rt-build.sh
		cd streaming-processor
		mvn clean package
		supervisord -c target/classes/supervisord.conf

## Eclipse Development

1. Install eGit plugin
2. Install Maven Integration for Eclipse (m2e)
3. Use the Eclipse `Git Repository Exploring` perspective
4. Click the `Clone a Git Repository`
5. Use the URI `https://github.com/stucco/rt.git`
6. Right click the repository and select `Import Projects...`
7. Select `Import Existing Projects` and the pom.xml should be shown

## To Do

1. Implement configuration with etcd, defaulting to local config.yaml if etcd is not running
2. Add the RMQ message's timestamp to the subgraph that is passed to alignment
3. Implement doc-service-java-client method to fetch extracted text from doc-service, and use this method in the UnstructuredTransformer