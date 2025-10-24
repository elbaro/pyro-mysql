flame:
    export PYTHONPERFSUPPORT=1
    sudo sysctl  kernel.kptr_restrict=0
    sudo sysctl  kernel.perf_event_paranoid=-1
    cargo flamegraph --bench bench_pyro --no-default-features
