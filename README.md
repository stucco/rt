# rt

## Build Status

master: [![Build Status](https://travis-ci.org/stucco/rt.png?branch=master)]
(https://travis-ci.org/stucco/rt)
dev: [![Build Status](https://travis-ci.org/stucco/rt.png?branch=dev)]
(https://travis-ci.org/stucco/rt)

## Running Topology

### Prerequisites
* RabbitMQ running on the default port 5672
* [document-service](https://github.com/stucco/document-service) running on the default port 4001
* Titan/query-service running

### Compile and Execute Locally
1. Open a terminal and cd to the rt directory
2. Run the following commands:
	
		./maven-rt-build.sh
		cd stucco-topology
		mvn clean package
		mvn exec:java

### Package for Deployment to Cluster
1. Modify the stucco-topology/pom.xml storm dependency section to indicate that the environment will provide the Storm library at runtime, instead of packaging it within the stucco-topology jar as with local execution:

		<dependency>
			<groupId>storm</groupId>
			<artifactId>storm</artifactId>
			<version>[0.9.0.1,)</version>
			<scope>provided</scope>
		</dependency>
	
2. Follow steps above in the [Compile and Execute Locally](#Compile and Execute Locally) section

## Eclipse Development

1. Install eGit plugin
2. Install Maven Integration for Eclipse (m2e)
3. Use the Eclipse `Git Repository Exploring` perspective
4. Click the `Clone a Git Repository`
5. Use the URI `https://github.com/stucco/rt.git`
6. Right click the repository and select `Import Projects...`
7. Select `Import Existing Projects` and the pom.xml should be shown