use std::ffi::CString;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use mysql::{prelude::Queryable, TxOpts};
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

pub fn bench_sqlalchemy(c: &mut Criterion) {
    setup_db();

    // Load SQLAlchemy benchmark functions
    Python::attach(|py| {
        Python::run(
            py,
            c_str!(include_str!("./bench_sqlalchemy.py")),
            None,
            None,
        )
        .unwrap();
    });

    // Benchmark INSERT operations (individual)
    {
        let mut group = c.benchmark_group("SQLAlchemy INSERT (individual)");

        for (name, statement) in [
            ("pyro_mysql", c_str!("insert_individual('pyro_mysql', 100)")),
            ("pymysql", c_str!("insert_individual('pymysql', 100)")),
            ("mysqldb", c_str!("insert_individual('mysqldb', 100)")),
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
        let mut group = c.benchmark_group(format!("SQLAlchemy SELECT (batch {})", select_size));

        for (name, statement) in [
            (
                "pyro_mysql",
                CString::new(format!("select_query('pyro_mysql', 100, {select_size})")).unwrap(),
            ),
            (
                "pymysql",
                CString::new(format!("select_query('pymysql', 100, {select_size})")).unwrap(),
            ),
            (
                "mysqldb",
                CString::new(format!("select_query('mysqldb', 100, {select_size})")).unwrap(),
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
        let mut group = c.benchmark_group("SQLAlchemy UPDATE (individual)");

        for (name, statement) in [
            ("pyro_mysql", c_str!("update_individual('pyro_mysql', 100)")),
            ("pymysql", c_str!("update_individual('pymysql', 100)")),
            ("mysqldb", c_str!("update_individual('mysqldb', 100)")),
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

    // Benchmark UPDATE operations (bulk)
    {
        let mut group = c.benchmark_group("SQLAlchemy UPDATE (bulk)");

        for (name, statement) in [
            ("pyro_mysql", c_str!("update_bulk('pyro_mysql', 1000)")),
            ("pymysql", c_str!("update_bulk('pymysql', 1000)")),
            ("mysqldb", c_str!("update_bulk('mysqldb', 1000)")),
        ] {
            group.bench_function(name, |b| {
                b.iter_batched_ref(
                    || populate_table(1000),
                    |()| {
                        Python::attach(|py| {
                            py.eval(statement, None, None).unwrap();
                        });
                    },
                    BatchSize::PerIteration,
                )
            });
        }
    }
}

criterion_group!(benches, bench_sqlalchemy);
criterion_main!(benches);
