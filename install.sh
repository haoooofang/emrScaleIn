#!/bin/bash
# EMR Auto-Scaling Service Installation Script

set -e

# Default installation paths
INSTALL_DIR="/opt/emr-scaling"
CONFIG_DIR="/etc/emr-scaling"
SERVICE_FILE="/etc/systemd/system/emr-scaling.service"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --config-dir)
            CONFIG_DIR="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --install-dir DIR   Installation directory (default: /opt/emr-scaling)"
            echo "  --config-dir DIR    Configuration directory (default: /etc/emr-scaling)"
            echo "  --help              Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $key"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "=== EMR Auto-Scaling Service Installation ==="
echo "Installation directory: $INSTALL_DIR"
echo "Configuration directory: $CONFIG_DIR"

# Check if running as root
if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root" >&2
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
pip3 install boto3 pyyaml

# Create directories
echo "Creating directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$CONFIG_DIR"
mkdir -p "$INSTALL_DIR/logs"

# Copy files
echo "Copying files..."
cp -r ./src "$INSTALL_DIR/"
cp ./main.py "$INSTALL_DIR/"
cp ./requirements.txt "$INSTALL_DIR/"

# Copy configuration if it doesn't exist
if [ ! -f "$CONFIG_DIR/config.yaml" ]; then
    echo "Copying default configuration..."
    cp ./config.yaml "$CONFIG_DIR/"
else
    echo "Configuration file already exists, not overwriting"
fi

# Set permissions
echo "Setting permissions..."
chmod +x "$INSTALL_DIR/main.py"
chown -R ec2-user:ec2-user "$INSTALL_DIR"
chown -R ec2-user:ec2-user "$CONFIG_DIR"

# Install service
echo "Installing systemd service..."
cp ./emr-scaling.service "$SERVICE_FILE"

# Update service file with correct paths
sed -i "s|/opt/emr-scaling|$INSTALL_DIR|g" "$SERVICE_FILE"
sed -i "s|/etc/emr-scaling|$CONFIG_DIR|g" "$SERVICE_FILE"

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload

echo "Installation complete!"
echo ""
echo "To start the service:"
echo "  systemctl start emr-scaling"
echo ""
echo "To enable the service to start on boot:"
echo "  systemctl enable emr-scaling"
echo ""
echo "To check the service status:"
echo "  systemctl status emr-scaling"
echo ""
echo "To view logs:"
echo "  journalctl -u emr-scaling"
echo ""
echo "Configuration file location:"
echo "  $CONFIG_DIR/config.yaml"
echo ""
echo "NOTE: Please update the configuration file with your EMR cluster ID and region before starting the service."
