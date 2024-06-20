#!/bin/bash

set -e

# Configuration
INSTALL_DIR="$HOME/pastel_decentralized_datomic"
LOGFILE="$INSTALL_DIR/pastel_decentralized_datomic.log"
LOGDIR="$INSTALL_DIR/old_log_files"
PASTEL_CLI_DIR="$HOME/pastel"
PASTEL_CLI="$PASTEL_CLI_DIR/pastel-cli"
DATOMIC_VERSION="1.0.7075"
DATOMIC_DIR="$INSTALL_DIR/datomic-pro-$DATOMIC_VERSION"
CASSANDRA_VERSION="4.1.1"
CASSANDRA_DIR="$INSTALL_DIR/apache-cassandra-$CASSANDRA_VERSION"
DATOMIC_TRANSACTOR_PID_FILE="$INSTALL_DIR/datomic_transactor.pid"
SYNC_INTERVAL=10  # Time interval in seconds for periodic syncing
DATOMIC_PORT=4334
CASSANDRA_PORT=9042
CASSANDRA_RPC_PORT=9160

# Function to log messages
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOGFILE"
}

# Rotate log files
rotate_logs() {
    if [ -f "$LOGFILE" ] && [ $(stat -c%s "$LOGFILE") -ge 10485760 ]; then
        log_message "Rotating log file..."
        mkdir -p "$LOGDIR"
        mv "$LOGFILE" "$LOGDIR/pastel_decentralized_datomic_$(date +%Y%m%d%H%M%S).log"
        touch "$LOGFILE"
    fi
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Install system dependencies
install_system_dependencies() {
    log_message "Updating and installing system dependencies..."
    sudo apt-get update
    sudo apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
    xz-utils tk-dev libffi-dev liblzma-dev python3-openssl git openjdk-11-jdk jq bc netcat
}

# Install and configure pyenv
install_pyenv() {
    if ! command_exists pyenv; then
        log_message "Installing pyenv..."
        git clone https://github.com/pyenv/pyenv.git ~/.pyenv
        echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
        echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
        echo 'eval "$(pyenv init --path)"' >> ~/.bashrc
        source ~/.bashrc
    else
        log_message "Updating pyenv..."
        cd ~/.pyenv && git pull && cd -
    fi
}

# Setup Python environment
setup_python_env() {
    log_message "Setting up Python environment..."
    pyenv install -s 3.12.0
    mkdir -p "$INSTALL_DIR"
    cd "$INSTALL_DIR"
    pyenv local 3.12.0
    python -m venv venv
    source venv/bin/activate
    python -m pip install --upgrade pip wheel setuptools
    python -m pip install fastapi uvicorn httpx datomic-client cassandra-driver
}

# Download and install Datomic Pro
install_datomic() {
    if [ ! -d "$DATOMIC_DIR" ]; then
        log_message "Downloading and extracting Datomic Pro..."
        wget -q https://datomic-pro-downloads.s3.amazonaws.com/$DATOMIC_VERSION/datomic-pro-$DATOMIC_VERSION.zip
        unzip -q datomic-pro-$DATOMIC_VERSION.zip -d "$INSTALL_DIR"
        rm datomic-pro-$DATOMIC_VERSION.zip
    else
        log_message "Datomic Pro is already installed."
    fi
}

# Download and install Cassandra
install_cassandra() {
    if [ ! -d "$CASSANDRA_DIR" ]; then
        log_message "Downloading and extracting Apache Cassandra..."
        wget -q https://downloads.apache.org/cassandra/$CASSANDRA_VERSION/apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz
        tar xzf apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz -C "$INSTALL_DIR"
        rm apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz
    else
        log_message "Apache Cassandra is already installed."
    fi
}

# Get list of Supernode IPs
get_supernode_ips() {
    log_message "Fetching Supernode IPs..."
    SUPERNODE_IPS=$($PASTEL_CLI masternode list full | jq -r 'to_entries | map(select(.value | contains("ENABLED"))) | map(.value | split(":") | .[0]) | .[]')
    echo "$SUPERNODE_IPS"
}

# Configure Cassandra for cluster
configure_cassandra() {
    local node_ip=$(hostname -I | awk '{print $1}')
    local seed_ips=$(get_supernode_ips | head -n 3 | tr '\n' ',' | sed 's/,$//')

    log_message "Configuring Cassandra for IP: $node_ip with seeds: $seed_ips"

    sed -i 's/^cluster_name:.*/cluster_name: '"'PastelDatomicCluster'"'/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^seed_provider:.*seed_provider:/seed_provider:\n    - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n      parameters:\n          - seeds: "'"$seed_ips"'"/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^listen_address:.*/listen_address: '"$node_ip"'/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^rpc_address:.*/rpc_address: '"$node_ip"'/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^endpoint_snitch:.*/endpoint_snitch: GossipingPropertyFileSnitch/' $CASSANDRA_DIR/conf/cassandra.yaml
    
    # Optimize for robustness and reliability
    sed -i 's/^read_request_timeout_in_ms:.*/read_request_timeout_in_ms: 10000/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^range_request_timeout_in_ms:.*/range_request_timeout_in_ms: 20000/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^write_request_timeout_in_ms:.*/write_request_timeout_in_ms: 10000/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^counter_write_request_timeout_in_ms:.*/counter_write_request_timeout_in_ms: 10000/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^cas_contention_timeout_in_ms:.*/cas_contention_timeout_in_ms: 5000/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^truncate_request_timeout_in_ms:.*/truncate_request_timeout_in_ms: 60000/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^request_timeout_in_ms:.*/request_timeout_in_ms: 20000/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^num_tokens:.*/num_tokens: 256/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^hinted_handoff_enabled:.*/hinted_handoff_enabled: true/' $CASSANDRA_DIR/conf/cassandra.yaml
    sed -i 's/^max_hint_window_in_ms:.*/max_hint_window_in_ms: 10800000/' $CASSANDRA_DIR/conf/cassandra.yaml
}

# Start Cassandra
start_cassandra() {
    log_message "Starting Cassandra..."
    $CASSANDRA_DIR/bin/cassandra -R
    sleep 30  # Wait for Cassandra to start up
}

# Configure Datomic
configure_datomic() {
    local node_ip=$(hostname -I | awk '{print $1}')
    log_message "Configuring Datomic..."
    cat << EOF > $DATOMIC_DIR/config/transactor.properties
protocol=cassandra
host=$node_ip
port=$CASSANDRA_PORT
alt-host=$node_ip
object-cache-max=2g
memory-index-threshold=64m
memory-index-max=512m
concurrency-level=8
write-concurrency=8
read-concurrency=8
heartbeat-interval-ms=5000
memcached=false
metrics-callback=true
validate-data-files=true
EOF
}

# Start Datomic transactor
start_datomic_transactor() {
    log_message "Starting Datomic transactor..."
    cd $DATOMIC_DIR
    nohup bin/transactor config/transactor.properties > $INSTALL_DIR/datomic_transactor.log 2>&1 &
    echo $! > $DATOMIC_TRANSACTOR_PID_FILE
    log_message "Datomic transactor started with PID: $(cat $DATOMIC_TRANSACTOR_PID_FILE)"
}

# Stop Datomic transactor
stop_datomic_transactor() {
    log_message "Stopping Datomic transactor..."
    if [ -f "$DATOMIC_TRANSACTOR_PID_FILE" ]; then
        kill $(cat $DATOMIC_TRANSACTOR_PID_FILE) && rm $DATOMIC_TRANSACTOR_PID_FILE
        log_message "Datomic transactor stopped."
    else
        log_message "No Datomic transactor PID file found. Nothing to stop."
    fi
}

# Function to determine if this node is the current transactor
is_current_transactor() {
    log_message "Determining if this node is the current transactor..."
    CURRENT_BLOCK_HASH=$($PASTEL_CLI getbestblockhash)
    CURRENT_BLOCK=$($PASTEL_CLI getblock $CURRENT_BLOCK_HASH)
    CURRENT_MERKLE_ROOT=$(echo $CURRENT_BLOCK | jq -r .merkleroot)
    PASTEL_ID=$($PASTEL_CLI pastelid list | jq -r 'keys[0]')
    
    merkle_root_hash=$(echo -n $CURRENT_MERKLE_ROOT | sha256sum | awk '{print $1}')
    pastel_id_hash=$(echo -n $PASTEL_ID | sha256sum | awk '{print $1}')
    xor_distance=$(printf "%064x\n" $((0x$merkle_root_hash ^ 0x$pastel_id_hash)))
    
    # Get masternode list
    MASTERNODES=$($PASTEL_CLI masternode list extra)
    
    for node_id in $(echo $MASTERNODES | jq -r 'keys[]'); do
        node_id_hash=$(echo -n $node_id | sha256sum | awk '{print $1}')
        node_xor_distance=$(printf "%064x\n" $((0x$merkle_root_hash ^ 0x$node_id_hash)))
        if [ "$((16#$node_xor_distance))" -lt "$((16#$xor_distance))" ]; then
            log_message "This node is not the current transactor."
            return 1
        fi
    done
    
    log_message "This node is the current transactor."
    return 0
}

# Wait for Cassandra cluster to be ready
wait_for_cassandra_cluster() {
    log_message "Waiting for Cassandra cluster to be ready..."
    local max_attempts=30
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if $CASSANDRA_DIR/bin/nodetool status | grep -q "UN"; then
            log_message "Cassandra cluster is ready."
            return 0
        fi
        attempt=$((attempt+1))
        log_message "Waiting for Cassandra cluster... Attempt $attempt/$max_attempts"
        sleep 10
    done
    log_message "Cassandra cluster failed to become ready in time."
    return 1
}

# Main function
main() {
    mkdir -p "$INSTALL_DIR"
    cd "$INSTALL_DIR"
    rotate_logs
    install_system_dependencies
    install_pyenv
    setup_python_env
    install_datomic
    install_cassandra
    configure_cassandra
    start_cassandra
    wait_for_cassandra_cluster
    configure_datomic

    # Main loop
    while true; do
        if is_current_transactor; then
            if [ ! -f "$DATOMIC_TRANSACTOR_PID_FILE" ]; then
                start_datomic_transactor
            fi
        else
            if [ -f "$DATOMIC_TRANSACTOR_PID_FILE" ]; then
                stop_datomic_transactor
            fi
        fi

        # Sleep for the specified interval before checking again
        log_message "Sleeping for $SYNC_INTERVAL seconds before the next check..."
        sleep $SYNC_INTERVAL
    done
}

# Run the main function
main