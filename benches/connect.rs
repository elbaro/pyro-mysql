use criterion::{Criterion, criterion_group, criterion_main};
use pyo3::{ffi::c_str, prelude::*};

pub fn bench(c: &mut Criterion) {
    Python::attach(|py| {
        Python::run(py, c_str!(include_str!("./connect.py")), None, None).unwrap();
    });

    let mut group = c.benchmark_group("Connect");

    // Sync drivers
    for (name, statement) in [
        ("mysqlclient (sync)", c"connect_mysqldb()"),
        ("pymysql (sync)", c"connect_pymysql()"),
        ("pyro/mysql (sync)", c"connect_pyro_sync()"),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                Python::attach(|py| {
                    py.eval(statement, None, None).unwrap();
                });
            })
        });
    }

    // Async drivers
    for (name, statement) in [
        (
            "pyro/mysql (async)",
            c"loop.run_until_complete(connect_pyro_async())",
        ),
        (
            "pyro/wtx (async)",
            c"loop.run_until_complete(connect_pyro_wtx())",
        ),
        (
            "asyncmy (async)",
            c"loop.run_until_complete(connect_asyncmy())",
        ),
        (
            "aiomysql (async)",
            c"loop.run_until_complete(connect_aiomysql())",
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                Python::attach(|py| {
                    py.eval(statement, None, None).unwrap();
                });
            })
        });
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
