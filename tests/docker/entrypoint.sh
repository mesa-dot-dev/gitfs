#!/bin/sh
MESAFS_LOG=debug nohup mesafs --config-path /etc/mesafs/config.toml run \
    > /tmp/mesafs-stdout.log 2> /tmp/mesafs-stderr.log &

# Wait for FUSE mount, then hand off to CMD
elapsed=0
while [ "${elapsed}" -lt 60 ]; do
    if [ -d /mnt/mesafs/github ]; then
        touch /tmp/mesafs-ready
        exec "$@"
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

echo "mesafs mount did not become ready within 60s" >&2
exit 1
