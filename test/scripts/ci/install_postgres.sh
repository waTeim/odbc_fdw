#!/bin/bash

set -ex

# Remove those all PgSQL versions except the one we're testing
PGSQL_VERSIONS=(9.2 9.3 9.4 9.5 9.6 10 11)
/etc/init.d/postgresql stop # stop travis default instance
for V in "${PGSQL_VERSIONS[@]}"; do
    if [ "$V" != "$PGSQL_VERSION" ]; then
        sudo apt-get -y remove --purge postgresql-${V}
    fi
    echo $i
done
apt-get -y autoremove

# Install PostgreSQL 
apt-get -y install postgresql-$PGSQL_VERSION
apt-get -y install postgresql-server-dev-$PGSQL_VERSION
/etc/init.d/postgresql restart $PGSQL_VERSION

# configure it to accept local connections from postgres
echo -e "# TYPE  DATABASE        USER            ADDRESS                 METHOD \nlocal   all             postgres                                trust\nlocal   all             all                                     trust\nhost    all             all             127.0.0.1/32            trust" \
  | tee /etc/postgresql/$PGSQL_VERSION/main/pg_hba.conf

