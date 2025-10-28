bench-setup:
    export PYTHONPERFSUPPORT=1
    sudo sysctl  kernel.kptr_restrict=0
    sudo sysctl  kernel.perf_event_paranoid=-1
    # cargo flamegraph --bench bench_pyro --no-default-features

release:
    RUSTFLAGS="--remap-path-prefix $HOME/.cargo=.cargo --remap-path-prefix $HOME/.rustup=.rustup --remap-path-prefix {{ justfile_directory() }}=." cargo build --release --lib
    mv target/release/libpyro_mysql.so pyro_mysql/pyro_mysql.abi3.so
    patchelf --remove-rpath pyro_mysql/pyro_mysql.abi3.so

bench:
    PYTHONPATH=. cargo bench --bench bench --no-default-features

bench-async:
    PYTHONPATH=. cargo bench --bench bench pyro-async --no-default-features

microbench:
    RUSTFLAGS="-Cforce-frame-pointers=yes" cargo build --profile=profiling --bin microbench --no-default-features
    PYTHONPERFSUPPORT=1 PYTHONPATH=. samply record ./target/profiling/microbench

microbench2:
    RUSTFLAGS="-Cforce-frame-pointers=yes" cargo build --profile=profiling --bin microbench --no-default-features
    PYTHONPERFSUPPORT=1 PYTHONPATH=. perf record -g ./target/profiling/microbench

perf:
    PYTHONPERFSUPPORT=1 PYTHONPATH=. perf record -g --call-graph dwarf -o perf.data ./target/profiling/microbench
