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
        Python::run(py, c_str!(include_str!("./bench.py")), None, None).unwrap();
    });

    for select_size in [1, 10, 100] {
        let mut group = c.benchmark_group(format!("SELECT {}", select_size));

        for (name, statement) in [
            (
                "mysqlclient",
                CString::new(format!("select_sync(MySQLdb.connect, 100, {select_size})")).unwrap(),
            ),
            (
                "pymysql",
                CString::new(format!("select_sync(pymysql.connect, 100, {select_size})")).unwrap(),
            ),
            (
                "pyro-sync",
                CString::new(format!("select_pyro_sync(100, {select_size})")).unwrap(),
            ),
            (
                "pyro-async",
                CString::new(format!(
                    "loop.run_until_complete(select_pyro_async(100, {select_size}))"
                ))
                .unwrap(),
            ),
            (
                "asyncmy",
                CString::new(format!(
                    "loop.run_until_complete(select_async(asyncmy.connect, 100, {select_size}))"
                ))
                .unwrap(),
            ),
            (
                "aiomysql",
                CString::new(format!(
                    "loop.run_until_complete(select_async(aiomysql.connect, 100, {select_size}))"
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
    {
        let mut group = c.benchmark_group("INSERT");

        for (name, statement) in [
            ("mysqlclient", c_str!("insert_sync(MySQLdb.connect, 100)")),
            ("pymysql", c_str!("insert_sync(pymysql.connect, 100)")),
            ("pyro-sync", c_str!("insert_pyro_sync(100)")),
            (
                "pyro-async",
                c_str!("loop.run_until_complete(insert_pyro_async(100))"),
            ),
            (
                "asyncmy",
                c_str!("loop.run_until_complete(insert_async(asyncmy.connect, 100))"),
            ),
            (
                "aiomysql",
                c_str!("loop.run_until_complete(insert_async(aiomysql.connect, 100))"),
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
}

criterion_group!(benches, bench);
criterion_main!(benches);
