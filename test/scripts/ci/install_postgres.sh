#!/bin/bash

# echo commands
set -x

# exit on error
set -e

# Add the PDGD repository
sudo apt-key adv --keyserver keys.gnupg.net --recv-keys ACCC4CF8
add-apt-repository "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main"
apt-get update

# Remove those all PgSQL versions except the one we're testing
PGSQL_VERSIONS=(9.2 9.3 9.4 9.5 9.6 10 11)
/etc/init.d/postgresql stop # stop travis default instance
for V in "${PGSQL_VERSIONS[@]}"; do
    if [ "$V" != "$PGSQL_VERSION" ]; then
        apt-get -y remove --purge postgresql-${V}
    fi
done
apt-get -y autoremove

# Install PostgreSQL
apt-get -y install postgresql-${PGSQL_VERSION} postgresql-server-dev-${PGSQL_VERSION}

# Configure it to accept local connections from postgres
echo -e "# TYPE  DATABASE        USER            ADDRESS                 METHOD \nlocal   all             postgres                                trust\nlocal   all             all                                     trust\nhost    all             all             127.0.0.1/32            trust" > /etc/postgresql/${PGSQL_VERSION}/main/pg_hba.conf

# Restart PostgreSQL 
/etc/init.d/postgresql restart ${PGSQL_VERSION}
