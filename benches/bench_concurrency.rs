use std::ffi::CString;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use mysql::{TxOpts, prelude::Queryable};
use pyo3::{ffi::c_str, prelude::*};

fn setup_db() {
    let mut conn = mysql::Conn::new("mysql://test:1234@127.0.0.1:3306/test").unwrap();
    conn.exec_drop("DROP TABLE IF EXISTS benchmark_test", ())
        .unwrap();
    conn.exec_drop(
        "CREATE TABLE benchmark_test (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100),
            age INT,
            email VARCHAR(100),
            score FLOAT,
            description VARCHAR(100)
        ) ENGINE = MEMORY",
        (),
    )
    .unwrap();
}

fn populate_table(n: usize) {
    let mut conn = mysql::Conn::new("mysql://test:1234@127.0.0.1:3306/test").unwrap();
    conn.exec_drop("TRUNCATE TABLE benchmark_test", ()).unwrap();
    {
        let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
        for i in 0..n {
            tx.exec_drop(
                "INSERT INTO benchmark_test (name, age, email, score, description)
                       VALUES (?, ?, ?, ?, ?)",
                (
                    format!("user_{i}"),
                    20 + (i % 50),
                    format!("user{i}@example.com"),
                    (i % 100) as f32,
                    format!("User description {i}"),
                ),
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
}

pub fn bench(c: &mut Criterion) {
    setup_db();

    Python::attach(|py| {
        Python::run(
            py,
            c_str!(include_str!("./bench_concurrency.py")),
            None,
            None,
        )
        .unwrap();
    });

    // Concurrent benchmarks with varying concurrency levels (same ~1000 total rows)
    for concurrency in [1, 3, 5, 10] {
        let group_name = format!("Concurrent SELECT {}", concurrency);
        let mut group = c.benchmark_group(&group_name);

        for (name, statement) in [
            (
                "pyro-async",
                format!(
                    "loop.run_until_complete(concurrent_select_pyro_async({}))",
                    concurrency
                ),
            ),
            (
                "asyncmy",
                format!(
                    "loop.run_until_complete(concurrent_select_async(asyncmy.connect, {}))",
                    concurrency
                ),
            ),
            (
                "aiomysql",
                format!(
                    "loop.run_until_complete(concurrent_select_async(aiomysql.connect, {}))",
                    concurrency
                ),
            ),
        ] {
            let statement = CString::new(statement).unwrap();
            group.bench_function(name, |b| {
                b.iter_batched(
                    || populate_table(1000),
                    |()| {
                        Python::attach(|py| {
                            py.eval(&statement, None, None).unwrap();
                        });
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }

    // Concurrent INSERT benchmarks with varying concurrency levels (same ~1000 total inserts)
    for concurrency in [1, 3, 5, 10] {
        let group_name = format!("Concurrent INSERT {}", concurrency);
        let mut group = c.benchmark_group(&group_name);

        for (name, statement) in [
            (
                "pyro-async",
                format!(
                    "loop.run_until_complete(concurrent_insert_pyro_async({}))",
                    concurrency
                ),
            ),
            (
                "asyncmy",
                format!(
                    "loop.run_until_complete(concurrent_insert_async(asyncmy.connect, {}))",
                    concurrency
                ),
            ),
            (
                "aiomysql",
                format!(
                    "loop.run_until_complete(concurrent_insert_async(aiomysql.connect, {}))",
                    concurrency
                ),
            ),
        ] {
            let statement = CString::new(statement).unwrap();
            group.bench_function(name, |b| {
                b.iter_batched(
                    || {
                        let mut conn = mysql::Conn::new("mysql://test:1234@127.0.0.1:3306/test").unwrap();
                        conn.exec_drop("TRUNCATE TABLE benchmark_test", ()).unwrap();
                    },
                    |()| {
                        Python::attach(|py| {
                            py.eval(&statement, None, None).unwrap();
                        });
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
