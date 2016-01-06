#!/bin/bash
set -e

if [ ! -e /opt/wp-config.php ]; then

echo "define('DB_NAME', '${WORDPRESS_DB_NAME}');" >> /opt/wp-config.php \
  && echo "define('DB_USER', '${WORDPRESS_DB_USER}');" >> /opt/wp-config.php \
  && echo "define('DB_PASSWORD', '${WORDPRESS_DB_PASSWORD}');" >> /opt/wp-config.php \
  && echo "define('DB_HOST', '${WORDPRESS_DB_HOST}');" >> /opt/wp-config.php \
  && echo "\$table_prefix = '${WORDPRESS_DB_PREFIX}';" >> /opt/wp-config.php
    
fi

while ! exec 6<>/dev/tcp/${MYSQL_PORT_3306_TCP_ADDR}/${MYSQL_PORT_3306_TCP_PORT}; do
  echo "Waiting for MySQL to be ready"
  sleep 1
done
exec 6>&-
exec 6<&-

exec "$@"