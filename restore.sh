#!/bin/sh
set -e

BACKUP_DIR="./backups"

if [ -z "$1" ]; then
    echo "Usage: ./restore.sh <backup-file>"
    echo ""
    echo "Available backups:"
    ls -1t "$BACKUP_DIR"/recipes_*.db 2>/dev/null || echo "  No backups found."
    exit 1
fi

BACKUP_FILE="$1"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "Error: File not found: $BACKUP_FILE"
    exit 1
fi

echo "This will replace the live database with: $BACKUP_FILE"
printf "Continue? [y/N] "
read -r CONFIRM
if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
    echo "Aborted."
    exit 0
fi

echo "Stopping app..."
docker compose stop trmnl-trending-recipes

echo "Restoring backup..."
docker compose run --rm db-backup cp "/backups/$(basename "$BACKUP_FILE")" /data/recipes.db

echo "Starting app..."
docker compose start trmnl-trending-recipes

echo "Restore complete."
