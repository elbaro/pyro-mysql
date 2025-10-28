flame:
    export PYTHONPERFSUPPORT=1
    sudo sysctl  kernel.kptr_restrict=0
    sudo sysctl  kernel.perf_event_paranoid=-1
    cargo flamegraph --bench bench_pyro --no-default-features

release:
    RUSTFLAGS="--remap-path-prefix $HOME/.cargo=.cargo --remap-path-prefix $HOME/.rustup=.rustup" cargo build --release --lib
    # RUSTFLAGS="--remap-path-prefix $HOME/.cargo=.cargo --remap-path-prefix $HOME/.rustup=.rustup --remap-path-prefix {{ justfile_directory() }}=." cargo build --release --lib

    mv target/release/libpyro_mysql.so pyro_mysql/pyro_mysql.abi3.so
    patchelf --remove-rpath pyro_mysql/pyro_mysql.abi3.so
