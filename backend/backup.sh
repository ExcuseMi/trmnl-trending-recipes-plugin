#!/bin/sh

BACKUP_DIR="/backups"
DB_PATH="/data/recipes.db"
CHECK_INTERVAL=3600          # seconds between checks (1 hour)
RETENTION_DAYS="${RETENTION_DAYS:-7}"

mkdir -p "$BACKUP_DIR"

while true; do
    TODAY=$(date +%Y-%m-%d)
    BACKUP_FILE="$BACKUP_DIR/recipes_$TODAY.db"

    if [ ! -f "$DB_PATH" ]; then
        echo "[$(date)] Database not found at $DB_PATH, waiting..."
    elif [ -f "$BACKUP_FILE" ]; then
        echo "[$(date)] Today's backup already exists, skipping."
    else
        echo "[$(date)] Starting backup..."
        sqlite3 "$DB_PATH" ".backup '$BACKUP_FILE'"
        echo "[$(date)] Backup created: $BACKUP_FILE ($(du -h "$BACKUP_FILE" | cut -f1))"

        # Prune backups older than 7 days
        DELETED=$(find "$BACKUP_DIR" -name "recipes_*.db" -mtime +$RETENTION_DAYS -print -delete)
        if [ -n "$DELETED" ]; then
            echo "[$(date)] Pruned old backups:"
            echo "$DELETED"
        fi

        echo "[$(date)] Backup complete."
    fi

    sleep $CHECK_INTERVAL
done
