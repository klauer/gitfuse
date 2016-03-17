mkdir mnt

export OAUTH_TOKEN=$(cat token.txt)

fusermount -u mnt
# strace -s 1024 python -m gitfuse.fs mnt 2> trace_log.txt
# python -m gitfuse.fs mnt
python -m gitfuse.githubfs -v mnt --users=klauer --orgs=nsls-ii --update-rate=30
