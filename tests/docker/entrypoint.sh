#!/bin/sh
GIT_FS_LOG=debug nohup git-fs --config-path /etc/git-fs/config.toml run \
    > /tmp/git-fs-stdout.log 2> /tmp/git-fs-stderr.log &

# Wait for FUSE mount, then hand off to CMD
elapsed=0
while [ "${elapsed}" -lt 60 ]; do
    if [ -d /mnt/git-fs/github ]; then
        touch /tmp/git-fs-ready
        exec "$@"
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

echo "git-fs mount did not become ready within 60s" >&2
exit 1
