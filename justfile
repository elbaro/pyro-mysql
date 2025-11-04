bench-setup:
    export PYTHONPERFSUPPORT=1
    sudo sysctl  kernel.kptr_restrict=0
    sudo sysctl  kernel.perf_event_paranoid=-1
    # cargo flamegraph --bench bench_pyro --no-default-features

release:
    RUSTFLAGS="--remap-path-prefix $HOME/.cargo=.cargo --remap-path-prefix $HOME/.rustup=.rustup --remap-path-prefix {{ justfile_directory() }}=." cargo build --release --lib
    mv target/release/libpyro_mysql.so pyro_mysql/pyro_mysql.abi3.so
    patchelf --remove-rpath pyro_mysql/pyro_mysql.abi3.so

bench: release
    PYTHONPATH=. cargo bench --bench bench --no-default-features

bench-concurrency: release
    PYTHONPATH=. cargo bench --bench bench_concurrency --no-default-features

bench-sqlalchemy: release
    PYTHONPATH=. cargo bench --bench bench_sqlalchemy --no-default-features
    PYTHONPATH=. cargo bench --bench bench_sqlalchemy_async --no-default-features

microbench:
    cargo build --profile=profiling --lib --no-default-features
    mv target/profiling/libpyro_mysql.so pyro_mysql/pyro_mysql.abi3.so
    cargo build --profile=profiling --bin microbench  --no-default-features
    PYTHONPERFSUPPORT=1 PYTHONPATH=. samply record ./target/profiling/microbench

microbench-sync:
    cargo build --profile=profiling --lib --no-default-features
    mv target/profiling/libpyro_mysql.so pyro_mysql/pyro_mysql.abi3.so
    cargo build --profile=profiling --bin microbench_sync  --no-default-features
    PYTHONPERFSUPPORT=1 PYTHONPATH=. samply record ./target/profiling/microbench_sync

callgrind:
    PYTHONPATH=. valgrind --tool=callgrind ./target/profiling/microbench 

fmt:
    cargo fmt
    black .

publish:
    rm -rf target/wheels
    maturin build
    7z e target/wheels/*.whl pyro_mysql/pyro_mysql.abi3.so -otarget/wheels/pyro_mysql
    patchelf --remove-rpath target/wheels/pyro_mysql/pyro_mysql.abi3.so
    cd target/wheels && 7z u *.whl pyro_mysql/pyro_mysql.abi3.so
    maturin upload target/wheels/*.whl

update-result:
    cp 'target/criterion/INSERT/report/violin.svg' 'report/INSERT.svg'
    cp 'target/criterion/SELECT_1/report/violin.svg' 'report/SELECT_1.svg'
    cp 'target/criterion/SELECT_10/report/violin.svg' 'report/SELECT_10.svg'
    cp 'target/criterion/SELECT_100/report/violin.svg' 'report/SELECT_100.svg'
    cp 'target/criterion/SELECT_1000/report/violin.svg' 'report/SELECT_1000.svg'
    cp 'target/criterion/SQLAlchemy Async INSERT/report/violin.svg' 'report/SQLAlchemy Async INSERT.svg'
    cp 'target/criterion/SQLAlchemy Async SELECT 1/report/violin.svg' 'report/SQLAlchemy Async SELECT 1.svg'
    cp 'target/criterion/SQLAlchemy Async SELECT 10/report/violin.svg' 'report/SQLAlchemy Async SELECT 10.svg'
    cp 'target/criterion/SQLAlchemy Async SELECT 100/report/violin.svg' 'report/SQLAlchemy Async SELECT 100.svg'
    cp 'target/criterion/SQLAlchemy Async SELECT 1000/report/violin.svg' 'report/SQLAlchemy Async SELECT 1000.svg'
    cp 'target/criterion/SQLAlchemy INSERT/report/violin.svg' 'report/SQLAlchemy INSERT.svg'
    cp 'target/criterion/SQLAlchemy SELECT 1/report/violin.svg' 'report/SQLAlchemy SELECT 1.svg'
    cp 'target/criterion/SQLAlchemy SELECT 10/report/violin.svg' 'report/SQLAlchemy SELECT 10.svg'
    cp 'target/criterion/SQLAlchemy SELECT 100/report/violin.svg' 'report/SQLAlchemy SELECT 100.svg'
    cp 'target/criterion/SQLAlchemy SELECT 1000/report/violin.svg' 'report/SQLAlchemy SELECT 1000.svg'
