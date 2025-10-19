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
            description VARCHAR(500)
        ) ENGINE = MEMORY",
        (),
    )
    .unwrap();
}

fn clear_table() {
    let mut conn = mysql::Conn::new("mysql://test:1234@127.0.0.1:3306/test").unwrap();
    conn.exec_drop("TRUNCATE TABLE benchmark_test", ()).unwrap();
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
                    20 + (i % 5),
                    format!("user{i}@example.com"),
                    (i % 10) as f32,
                    format!("Description for user {i}"),
                ),
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
}

pub fn bench_sqlalchemy_async(c: &mut Criterion) {
    setup_db();

    // Load SQLAlchemy async benchmark functions
    Python::attach(|py| {
        Python::run(
            py,
            c_str!(include_str!("./bench_sqlalchemy_async.py")),
            None,
            None,
        )
        .unwrap();
    });

    // Benchmark INSERT operations (individual)
    {
        let mut group = c.benchmark_group("SQLAlchemy Async INSERT (individual)");

        for (name, statement) in [
            (
                "pyro_mysql",
                c_str!("run_async_benchmark('pyro_mysql', 'insert_individual', 100)"),
            ),
            (
                "aiomysql",
                c_str!("run_async_benchmark('aiomysql', 'insert_individual', 100)"),
            ),
            (
                "asyncmy",
                c_str!("run_async_benchmark('asyncmy', 'insert_individual', 100)"),
            ),
        ] {
            group.bench_function(name, |b| {
                b.iter_batched(
                    || clear_table(),
                    |()| {
                        Python::attach(|py| {
                            py.eval(statement, None, None).unwrap();
                        });
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }

    // Benchmark SELECT operations
    for select_size in [1, 10, 100] {
        let mut group =
            c.benchmark_group(format!("SQLAlchemy Async SELECT (batch {})", select_size));

        for (name, statement) in [
            (
                "pyro_mysql",
                CString::new(format!(
                    "run_async_benchmark('pyro_mysql', 'select_query', 100, {select_size})"
                ))
                .unwrap(),
            ),
            (
                "aiomysql",
                CString::new(format!(
                    "run_async_benchmark('aiomysql', 'select_query', 100, {select_size})"
                ))
                .unwrap(),
            ),
            (
                "asyncmy",
                CString::new(format!(
                    "run_async_benchmark('asyncmy', 'select_query', 100, {select_size})"
                ))
                .unwrap(),
            ),
        ] {
            group.bench_function(name, |b| {
                b.iter_batched(
                    || populate_table(100),
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

    // Benchmark UPDATE operations (individual)
    {
        let mut group = c.benchmark_group("SQLAlchemy Async UPDATE (individual)");

        for (name, statement) in [
            (
                "pyro_mysql",
                c_str!("run_async_benchmark('pyro_mysql', 'update_individual', 100)"),
            ),
            (
                "aiomysql",
                c_str!("run_async_benchmark('aiomysql', 'update_individual', 100)"),
            ),
            (
                "asyncmy",
                c_str!("run_async_benchmark('asyncmy', 'update_individual', 100)"),
            ),
        ] {
            group.bench_function(name, |b| {
                b.iter_batched(
                    || populate_table(100),
                    |()| {
                        Python::attach(|py| {
                            py.eval(statement, None, None).unwrap();
                        });
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }
}

criterion_group!(benches, bench_sqlalchemy_async);
criterion_main!(benches);
