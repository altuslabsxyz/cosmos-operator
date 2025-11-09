set -eu

# $GENESIS_FILE and $CONFIG_DIR already set via pod env vars.

INIT_GENESIS_FILE="$WORK_DIR/.tmp/config/genesis.json"

# Ensure the config directory exists
echo "Ensuring config directory exists: $CONFIG_DIR"
mkdir -p "$CONFIG_DIR"

echo "Using initialized genesis file $INIT_GENESIS_FILE..."

set -x

mv "$INIT_GENESIS_FILE" "$GENESIS_FILE"

set +x

echo "Move complete."
