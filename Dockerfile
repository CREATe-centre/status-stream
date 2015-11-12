FROM debian:jessie

RUN DEBIAN_FRONTEND=noninteractive \
	&& apt-get clean \
	&& apt-get update \
	&& apt-get install -y openjdk-7-jre-headless openjdk-7-jdk maven

RUN rm -rf /var/lib/apt/lists/*

COPY src /opt/src
COPY pom.xml /opt

RUN cd /opt && mvn compile && mvn package

RUN echo "define('DB_NAME', '${WORDPRESS_DB_NAME}');" >> /opt/wp-config.php \
	&& echo "define('DB_USER', '${WORDPRESS_DB_USER}');" >> /opt/wp-config.php \
	&& echo "define('DB_PASSWORD', '${WORDPRESS_DB_PASSWORD}');" >> /opt/wp-config.php \
	&& echo "define('DB_HOST', '${WORDPRESS_DB_HOST}');" >> /opt/wp-config.php \
	&& echo "\$table_prefix = '${WORDPRESS_DB_PREFIX}';" >> /opt/wp-config.php

CMD ["/usr/bin/java", "-jar", "/opt/target/status-stream-0.0.1.jar", "/opt/wp-config.php"]