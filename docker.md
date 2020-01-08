# Install and run PostgreSQL / PostGIS Docker container

## Install docker

Download and install Docker using the appropriate link for your OS:

- [MacOS](https://download.docker.com/mac/stable/Docker.dmg)
- [Windows](https://download.docker.com/win/stable/Docker%20Desktop%20Installer.exe)


## Get the database container

Get the [container](https://hub.docker.com/r/crunchydata/crunchy-postgres-appdev):

`docker pull crunchydata/crunchy-postgres-appdev`

## Run the container

Modify and run this command as per your requirements (matching the parameters within the `db_url` variable in the `designatedlands.cfg` file):

`docker run -d -p 5432:5432 -e PG_USER=designatedlands -e PG_PASSWORD=password -e PG_DATABASE=designatedlands --name=pgsql crunchydata/crunchy-postgres-appdev`

Running the container like this:

1. Runs PostgreSQL in the background as a daemon
2. Allows you to connect to it on port 5433 from localhost or 127.0.0.1 (port number modified to avoid conflict with existing installations)
3. Sets the default user to designatedlands
4. Sets the password for this user *and * the postgres user to password
5. Creates a PostGIS and PL/R enabled database named designatedlands
6. Names the container pgsql

As long as you don't remove this container it will retain all the data you put in it. To start it up again:

 `docker start pgsql`