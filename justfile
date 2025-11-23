build:
    cargo build --release --lib
    mv target/release/libpyro_mysql.so pyro_mysql/pyro_mysql.abi3.so

check: build
    cargo fmt --check
    black --check pyro_mysql
    PYTHONPATH=. pytest

publish: check
    rm -rf target/wheels
    maturin build
    7z e target/wheels/*.whl pyro_mysql/pyro_mysql.abi3.so -otarget/wheels/pyro_mysql
    patchelf --remove-rpath target/wheels/pyro_mysql/pyro_mysql.abi3.so
    cd target/wheels && 7z u *.whl pyro_mysql/pyro_mysql.abi3.so
    maturin upload target/wheels/*.whl

# bump:
#     git cliff --bump

update-violin:
    cp 'target/criterion/INSERT/report/violin.svg' 'report/INSERT.svg'
    cp 'target/criterion/SELECT_1/report/violin.svg' 'report/SELECT_1.svg'
    cp 'target/criterion/SELECT_10/report/violin.svg' 'report/SELECT_10.svg'
    cp 'target/criterion/SELECT_100/report/violin.svg' 'report/SELECT_100.svg'
    cp 'target/criterion/SELECT_1000/report/violin.svg' 'report/SELECT_1000.svg'
    cp 'target/criterion/SQLAlchemy_Async_INSERT/report/violin.svg' 'report/SQLAlchemy_Async_INSERT.svg'
    cp 'target/criterion/SQLAlchemy_Async_SELECT_1/report/violin.svg' 'report/SQLAlchemy_Async_SELECT 1.svg'
    cp 'target/criterion/SQLAlchemy_Async_SELECT_10/report/violin.svg' 'report/SQLAlchemy_Async_SELECT 10.svg'
    cp 'target/criterion/SQLAlchemy_Async_SELECT_100/report/violin.svg' 'report/SQLAlchemy_Async_SELECT 100.svg'
    cp 'target/criterion/SQLAlchemy_Async_SELECT_1000/report/violin.svg' 'report/SQLAlchemy_Async_SELECT 1000.svg'
    cp 'target/criterion/SQLAlchemy_INSERT/report/violin.svg' 'report/SQLAlchemy_INSERT.svg'
    cp 'target/criterion/SQLAlchemy_SELECT_1/report/violin.svg' 'report/SQLAlchemy_SELECT_1.svg'
    cp 'target/criterion/SQLAlchemy_SELECT_10/report/violin.svg' 'report/SQLAlchemy_SELECT_10.svg'
    cp 'target/criterion/SQLAlchemy_SELECT_100/report/violin.svg' 'report/SQLAlchemy_SELECT_100.svg'
    cp 'target/criterion/SQLAlchemy_SELECT_1000/report/violin.svg' 'report/SQLAlchemy_SELECT_1000.svg'
