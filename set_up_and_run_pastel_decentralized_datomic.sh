#!/bin/bash

LOGFILE="pastel_decentralized_datomic.log"
LOGDIR="old_log_files"
USER_HOME=$(eval echo ~${SUDO_USER})
PASTEL_CLI_DIR="$USER_HOME/pastel"
PASTEL_CLI="$PASTEL_CLI_DIR/pastel-cli"
DATOMIC_VERSION=1.0.7075
DATOMIC_DIR="/opt/datomic-pro-$DATOMIC_VERSION"
DB_PATH="$DATOMIC_DIR/data/datomic.db"
TRANS_PROPS_PATH="$DATOMIC_DIR/transactor.properties"
TRANS_ACTOR_PID_FILE="/var/run/datomic_transactor.pid"
SYNC_INTERVAL=10  # Time interval in seconds for periodic syncing
UNISON_PORT=5000
UNISON_PROFILE="$USER_HOME/.unison/datomic.prf"

# Function to rotate log files
rotate_logs() {
  if [ -f "$LOGFILE" ] && [ $(stat -c%s "$LOGFILE") -ge 10485760 ]; then
    echo "Rotating log file..." | tee -a "$LOGFILE"
    mkdir -p "$LOGDIR"
    mv "$LOGFILE" "$LOGDIR/pastel_decentralized_datomic_$(date +%Y%m%d%H%M%S).log"
    touch "$LOGFILE"
  fi
}

# Log rotation check
rotate_logs

# Log and echo a message
log_and_echo() {
  echo "$1" | tee -a "$LOGFILE"
}

log_and_echo "Starting Datomic decentralized setup..."

# Update and install dependencies
log_and_echo "Updating and installing dependencies..."
sudo apt update | tee -a "$LOGFILE"
sudo apt upgrade -y | tee -a "$LOGFILE"
sudo apt install -y openjdk-11-jdk wget unzip sqlite3 jq unison bc | tee -a "$LOGFILE"

# Download and extract Datomic Pro
log_and_echo "Checking if Datomic Pro is already installed..."
if [ ! -d "$DATOMIC_DIR" ]; then
  log_and_echo "Datomic Pro not found. Downloading and extracting..."
  wget https://datomic-pro-downloads.s3.amazonaws.com/$DATOMIC_VERSION/datomic-pro-$DATOMIC_VERSION.zip | tee -a "$LOGFILE"
  unzip datomic-pro-$DATOMIC_VERSION.zip -d /opt | tee -a "$LOGFILE"
elif [ ! -f "$DATOMIC_DIR/bin/transactor" ]; then
  log_and_echo "Datomic Pro binary not found. Re-downloading and extracting..."
  wget https://datomic-pro-downloads.s3.amazonaws.com/$DATOMIC_VERSION/datomic-pro-$DATOMIC_VERSION.zip | tee -a "$LOGFILE"
  unzip -o datomic-pro-$DATOMIC_VERSION.zip -d /opt | tee -a "$LOGFILE"
else
  log_and_echo "Datomic Pro is already installed."
fi

# Ensure the data directory exists and has the correct permissions
log_and_echo "Ensuring data directory exists and has correct permissions..."
sudo mkdir -p $DATOMIC_DIR/data
sudo chown -R $SUDO_USER:$SUDO_USER $DATOMIC_DIR/data
chmod 700 $DATOMIC_DIR/data

# Create SQLite database if not exists
log_and_echo "Creating SQLite database if not exists..."
if [ ! -f "$DB_PATH" ]; then
  sudo -u $SUDO_USER sqlite3 $DB_PATH ".databases" | tee -a "$LOGFILE"
else
  log_and_echo "SQLite database already exists."
fi

# Create transactor properties file
log_and_echo "Creating transactor properties file..."
sudo tee $TRANS_PROPS_PATH > /dev/null <<EOF
protocol=sql
sql-url=jdbc:sqlite:$DB_PATH
sql-user=
sql-password=
sql-driver-class=org.sqlite.JDBC
memory-index-threshold=32m
memory-index-max=128m
object-cache-max=64m
EOF
sudo chown $SUDO_USER:$SUDO_USER $TRANS_PROPS_PATH
chmod 600 $TRANS_PROPS_PATH

# Ensure correct permissions for .unison directory on local machine
log_and_echo "Ensuring correct permissions for .unison directory..."
sudo rm -rf $USER_HOME/.unison
mkdir -p $USER_HOME/.unison
sudo chown -R $SUDO_USER:$SUDO_USER $USER_HOME/.unison
chmod 700 $USER_HOME/.unison

# Create a Unison profile
log_and_echo "Creating Unison profile..."
sudo -u $SUDO_USER tee $UNISON_PROFILE > /dev/null <<EOF
root = $DATOMIC_DIR/data
root = socket://<remote_ip>:$UNISON_PORT/$DATOMIC_DIR/data
auto = true
batch = true
EOF
sudo chown $SUDO_USER:$SUDO_USER $UNISON_PROFILE
chmod 600 $UNISON_PROFILE

# Create and enable systemd service for Unison TCP server
log_and_echo "Creating and enabling systemd service for Unison TCP server..."
sudo tee /etc/systemd/system/unison-datomic.service > /dev/null <<EOF
[Unit]
Description=Unison TCP Server for Datomic Synchronization
After=network.target

[Service]
ExecStart=/usr/bin/unison -socket $UNISON_PORT
Restart=always
User=$SUDO_USER
Group=$SUDO_USER

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload | tee -a "$LOGFILE"
sudo systemctl enable unison-datomic | tee -a "$LOGFILE"
sudo systemctl start unison-datomic | tee -a "$LOGFILE"

# Function to start the transactor
start_transactor() {
  log_and_echo "Starting Datomic transactor..."
  cd $DATOMIC_DIR
  sudo -u $SUDO_USER bin/transactor $TRANS_PROPS_PATH &
  echo $! | sudo tee $TRANS_ACTOR_PID_FILE > /dev/null
  log_and_echo "Datomic transactor started with PID: $(cat $TRANS_ACTOR_PID_FILE)"
}

# Function to stop the transactor
stop_transactor() {
  log_and_echo "Stopping Datomic transactor..."
  if [ -f "$TRANS_ACTOR_PID_FILE" ]; then
    sudo kill $(cat $TRANS_ACTOR_PID_FILE) && sudo rm $TRANS_ACTOR_PID_FILE
    log_and_echo "Datomic transactor stopped."
  else
    log_and_echo "No Datomic transactor PID file found. Nothing to stop."
  fi
}

# Function to sync the SQLite database using Unison over TCP
sync_database() {
  log_and_echo "Synchronizing SQLite database using Unison over TCP..."
  for node in "${NODE_IPS[@]}"; do
    if [ "$node" != "$(hostname -I | awk '{print $1}')" ]; then
      unison_command="sudo -u $SUDO_USER unison $USER_HOME/.unison/datomic.prf -logfile $USER_HOME/.unison/unison.log -debug all"
      log_and_echo "Running Unison command: $unison_command"
      eval $unison_command | tee -a "$LOGFILE"
    fi
  done
}

# Function to determine if this node is the current transactor
is_current_transactor() {
  log_and_echo "Determining if this node is the current transactor..."
  merkle_root_hash=$(echo -n $CURRENT_MERKLE_ROOT | sha256sum | awk '{print $1}')
  pastel_id_hash=$(echo -n $PASTEL_ID | sha256sum | awk '{print $1}')
  xor_distance=$(printf "%064x\n" $((0x$merkle_root_hash ^ 0x$pastel_id_hash)))
  closest_node=$PASTEL_ID

  # Get masternode list
  log_and_echo "Fetching masternode list..."
  MASTERNODES=$(sudo -u $SUDO_USER $PASTEL_CLI masternode list extra)
  FULL_MASTERNODES=$(sudo -u $SUDO_USER $PASTEL_CLI masternode list full)

  # Extract pastel ids and their IPs
  PASTEL_IDS=$(echo $MASTERNODES | jq -r 'keys[]')
  NODE_IPS=$(echo $FULL_MASTERNODES | jq -r 'to_entries | map(select(.value | index("ENABLED")) | .value | split(" ") | .[-1] | split(":") | .[0]) | .[]')

  for node_id in $PASTEL_IDS; do
    node_id_hash=$(echo -n $node_id | sha256sum | awk '{print $1}')
    node_xor_distance=$(printf "%064x\n" $((0x$merkle_root_hash ^ 0x$node_id_hash)))
    if [ "$((0x$node_xor_distance < 0x$xor_distance))" -eq 1 ]; then
      closest_node=$node_id
    fi
  done

  if [ "$closest_node" == "$PASTEL_ID" ]; then
    log_and_echo "This node is the current transactor."
    return 0  # This node is the current transactor
  else
    log_and_echo "This node is not the current transactor."
    return 1  # This node is not the current transactor
  fi
}

# Main loop to check for transactor status and sync database
log_and_echo "Entering main loop to check transactor status and sync database..."
while true; do
  rotate_logs
  if is_current_transactor; then
    if [ ! -f "$TRANS_ACTOR_PID_FILE" ]; then
      start_transactor
    fi
  else
    if [ -f "$TRANS_ACTOR_PID_FILE" ]; then
      stop_transactor
      # Sync the latest database from the current transactor
      for node in "${NODE_IPS[@]}"; do
        if [ "$node" != "$(hostname -I | awk '{print $1}')" ]; then
          log_and_echo "Fetching latest database from current transactor at $node..."
          sudo -u $SUDO_USER unison -batch -auto socket://$node:$UNISON_PORT/$DATOMIC_DIR/data $DATOMIC_DIR/data -logfile $USER_HOME/.unison/unison.log -debug all | tee -a "$LOGFILE"
          break
        fi
      done
    fi
  fi

  # Sync database periodically
  sync_database

  # Sleep for the specified interval before checking again
  log_and_echo "Sleeping for $SYNC_INTERVAL seconds before the next check..."
  sleep $SYNC_INTERVAL
done
