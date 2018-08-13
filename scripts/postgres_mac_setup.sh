#! bin/sh

# This writes settings to /usr/local/var/postgres/pgtune.conf to optimize
# postgres for running on big postgis databases. These settings have been 
# tested on a 2016 Macbook Pro with 16GB RAM.
# http://big-elephants.com/2012-12/tuning-postgres-on-macos/

cat << EOF > /usr/local/var/postgres/pgtune.conf
log_timezone = 'Canada/Pacific'
datestyle = 'iso, mdy'
timezone = 'Canada/Pacific'
lc_messages = 'en_US.UTF-8'			# locale for system error message
lc_monetary = 'en_US.UTF-8'			# locale for monetary formatting
lc_numeric = 'en_US.UTF-8'			# locale for number formatting
lc_time = 'en_US.UTF-8'				# locale for time formatting
default_text_search_config = 'pg_catalog.english'
default_statistics_target = 100
log_min_duration_statement = 2000

max_connections = 100
max_locks_per_transaction = 64
dynamic_shared_memory_type = posix
checkpoint_timeout = 30min		# range 30s-1d
maintenance_work_mem = 1GB
effective_cache_size = 6GB
work_mem = 500MB
max_wal_size = 10GB
wal_buffers = 16MB
shared_buffers = 4GB
EOF

# Edit /usr/local/var/postgres/postgresql.conf to read and load pgtune.conf

cat << EOF >> /usr/local/var/postgres/postgresql.conf
# Include custom settings:
include = 'pgtune.conf'
EOF

# Tune kernel settings to allow larger amounts of shared memory to facilitate
# parallel processing.
# https://www.postgresql.org/docs/10/static/kernel-resources.html
# https://benscheirman.com/2011/04/increasing-shared-memory-for-postgres-on-os-x/

sudo bash -c 'cat > /etc/sysctl.conf' << EOF
kern.sysv.shmmax=17179869184
kern.sysv.shmmin=1
kern.sysv.shmmni=32
kern.sysv.shmseg=8
kern.sysv.shmall=4194304
kern.maxprocperuid=512
kern.maxproc=2048
EOF
