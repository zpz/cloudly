from cloudly.gcp.compute import Instance, InstanceConfig

# def set_default_password(master_instance_name: str, username: str, password: str):
#     # Set credentials for the default user 'postgres'
#     # TODO: did not work in tests.
#     url = f'https://sqladmin.googleapis.com/sql/v1beta4/projects/{get_project_id()}/instances/{master_instance_name}/users?name={username}'
#     data = json.dumps({'name': username, 'password': password})
#     headers = {
#         'Content-Type': 'application/json',
#         'Authorization': f'Bearer {get_credentials().token}',
#     }
#     resp = requests.put(url, headers=headers, data=data, timeout=60)
#     assert resp.status_code == 200, resp.status_code


def attach_load_balancer(
    pg_master_instance_name: str,
    *,
    zone: str,
    postgres_version: str,
    machine_type: str | None = None,
    network_uri: str | None = None,
    subnet_uri: str | None = None,
):
    # See https://cloud.google.com/blog/products/databases/using-haproxy-to-scale-read-only-workloads-on-cloud-sql-for-postgresql
    # Most of the code below is copied from this link.

    config_backend_cfg = """\
listen pg-read-only
	bind *:5432
	option external-check
	external-check command /var/lib/haproxy/pgcheck.sh
	balance leastconn
"""

    config_global_cfg = """\
global
        log /dev/log    local0 alert
        log /dev/log    local1 notice alert
        stats socket /run/haproxy/admin.sock mode 660 level admin expose-fd listeners
        stats timeout 30s
        user haproxy
        group haproxy
        daemon
        external-check

defaults
        log global
        mode tcp
        retries 2
        timeout client 5m
        timeout server 5m
        timeout connect 5s
        timeout check 5s
"""

    pgcheck_sh = """\
#!/bin/bash

## Begin configuration block

PG_PSQL_CMD="/usr/bin/psql"

# Provide the username, database, and password for the health check user

PG_CONN_USER="enter health check username here"
PG_CONN_DB="postgres"
PG_CONN_PASSWORD="enter health check user password here"

## End configuration block

PG_CONN_HOST=$3
PG_CONN_PORT=$4

export PGPASSWORD=$PG_CONN_PASSWORD

PG_NODE_RESPONSE="$($PG_PSQL_CMD -t -A -d $PG_CONN_DB -U $PG_CONN_USER -h $PG_CONN_HOST -p $PG_CONN_PORT -c 'select 1')"

if [ "$PG_NODE_RESPONSE" == "1" ]; then
	echo 'Health check succeeded'
	exit 0
else
	echo 'Health check failed'
	exit 1
fi
"""

    cfgupdate_sh = """\
#!/bin/bash

## Begin configuration block

# Editable configuration, use your own primary instance name here
PG_PRIMARY_INSTANCE="Enter primary instance name here"

# Optional configuration. Provide HAProxy config file location (if different)
CFG_HAPROXY="/etc/haproxy/haproxy.cfg"

# Non-editable configuration
PG_REPLICAS_TMP_FILE="replicas.tmp"
LOG_FILE="cfgupdate.lastlog"
CFG_FILE_GLOBAL="config_global.cfg"
CFG_FILE_BACKEND="config_backend.cfg"
CFG_FILE_SERVERS="config_servers.cfg"
CFG_FILE_MERGED="config_merged.cfg"

## End configuration block
## Begin logic block

echo "NOTICE: HAProxy configuration update at" `date` > $LOG_FILE

# Describe the configured primary instance
# Read the list of replica names into a file, put the replica count in a variable

PG_REPLICA_COUNT=`gcloud sql instances describe $PG_PRIMARY_INSTANCE --format json 2>>$LOG_FILE |jq -r '.replicaNames | .[]' 2>>$LOG_FILE |tee $PG_REPLICAS_TMP_FILE |wc -l`

# Exit if the replica count is zero
# This is basic safety measure against e.g. transient API issues

if [ "$PG_REPLICA_COUNT" -eq "0" ]; then

	echo "ERROR: Replica count was found to be zero" |tee -a $LOG_FILE
	echo "ERROR: Make sure that $0 is configured with a correct primary instance name, and there is at least one replica" |tee -a $LOG_FILE
	exit 1

fi

# Iterate over replica names and obtain private IP for each one
# Write configuration into a temporary config file

# Empty the temporary config file containg replica IPs
echo -n "" > $CFG_FILE_SERVERS

while read -r REPLICA_NAME; do

	REPLICA_IP=`gcloud sql instances describe $REPLICA_NAME --format json | jq -r '.ipAddresses[] |select(.type=="PRIVATE") | .ipAddress'`

	# Exit if private IP not found

	if [ -z "$REPLICA_IP" ]; then
		echo "ERROR: Private IP not found for $REPLICA_NAME" |tee -a $LOG_FILE
		echo "ERROR: Make sure that Private IPs are configured for all read replicas" |tee -a $LOG_FILE
		continue
	fi

	echo "NOTICE: Detected replica $REPLICA_NAME at $REPLICA_IP" |tee -a $LOG_FILE
	echo -e "\tserver $REPLICA_NAME $REPLICA_IP:5432 check" >> $CFG_FILE_SERVERS

done < $PG_REPLICAS_TMP_FILE

# Merge the config files together
cat $CFG_FILE_GLOBAL $CFG_FILE_BACKEND $CFG_FILE_SERVERS > $CFG_FILE_MERGED

# Replace the HAProxy config file with the newly generated configuration
cp $CFG_FILE_MERGED $CFG_HAPROXY

# Reload HAProxy or start if not already running
# If the 1st script parameter is "dryrun", do nothing

if [ "$1" == "dryrun" ]; then

	echo "Dry run requested. Configuration file created in $CFG_FILE_MERGED but HAProxy config not updated."

else 

	if [ `pidof haproxy |wc -l` -eq 0 ]; then
		echo "Starting HAProxy"
		systemctl start haproxy
	else
		echo "Reloading HAPRoxy"
		systemctl reload haproxy
	fi

fi

## End logic block
"""

    script = f"""\
#!/bin/bash
sudo apt update
sudo apt install -y lsof jq socat wget gnupg2 
sudo apt install postgresql-{postgres_version} postgresql-client-{postgres_version} 
sudo apt install haproxy
sudo wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo mkdir -p /var/lib/haproxy

sudo tee /var/lib/haproxy/config_backend.cfg > /dev/null <<EOT
{config_backend_cfg}
EOT
sudo tee /var/lib/haproxy/config_global.cfg > /dev/null <<EOT
{config_global_cfg}
EOT
sudo tee /var/lib/haproxy/pgcheck.sh > /dev/null <<EOT
{pgcheck_sh}
EOT
sudo tee /var/lib/haproxy/cfgupdate.sh > /dev/null <<EOT
{cfgupdate_sh}
EOT

sudo chmod 777 /var/lib/haproxy/*
sudo /var/lib/haproxy/cfgupdate.sh {pg_master_instance_name}
"""
    Instance.create(
        InstanceConfig(
            name=f'haproxy-{pg_master_instance_name.replace("_", "-")}',
            zone=zone,
            network_uri=network_uri,
            subnet_uri=subnet_uri,
            machine_type=machine_type or 'n1-standard-1',
            startup_script=script,
        )
    )


def delete_load_balancer(pg_master_instance_name: str, zone: str):
    Instance(f'haproxy-{pg_master_instance_name.replace("_", "-")}', zone=zone).delete()
