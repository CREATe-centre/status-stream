FROM debian:jessie

RUN DEBIAN_FRONTEND=noninteractive \
	&& apt-get clean \
	&& apt-get update \
	&& apt-get install -y openjdk-7-jre-headless openjdk-7-jdk maven

RUN rm -rf /var/lib/apt/lists/*

COPY src /opt/src
COPY pom.xml /opt

RUN cd /opt && mvn compile && mvn package 

COPY docker-entrypoint.sh /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/usr/bin/java", "-jar", "/opt/target/status-stream-0.0.1.jar", "/opt/wp-config.php"]