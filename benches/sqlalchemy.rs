use criterion::{Criterion, criterion_group, criterion_main};
use mysql::{TxOpts, prelude::Queryable};
use pyo3::{ffi::c_str, prelude::*};

fn setup_db() {
    let mut conn = mysql::Conn::new("mysql://test:1234@localhost:3306/test").unwrap();
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
    let mut conn = mysql::Conn::new("mysql://test:1234@localhost:3306/test").unwrap();
    conn.exec_drop("TRUNCATE TABLE benchmark_test", ()).unwrap();
}

fn populate_table(n: usize) {
    let mut conn = mysql::Conn::new("mysql://test:1234@localhost:3306/test").unwrap();
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
        Python::run(py, c_str!(include_str!("./sqlalchemy.py")), None, None).unwrap();
    });

    // Benchmark SELECT operations
    for select_size in [1, 10, 100, 1000] {
        let mut group = c.benchmark_group(format!("SQLAlchemy_SELECT_{}", select_size));
        populate_table(select_size);

        for name in ["pyro/zero (sync)", "pymysql (sync)", "mysqldb (sync)"] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    Python::run(
                        py,
                        &std::ffi::CString::new(format!(
                            "session, engine = create_session('{name}')"
                        ))
                        .unwrap(),
                        None,
                        None,
                    )
                    .unwrap();
                    b.iter(|| py.eval(c"select_query()", None, None).unwrap());

                    Python::run(py, c"session.close(); engine.dispose()", None, None).unwrap();
                });
            });
        }
    }

    // Benchmark INSERT operations (individual)
    {
        let mut group = c.benchmark_group("SQLAlchemy_INSERT");

        for (name, stmt_template) in [
            ("pyro/zero (sync)", "insert_query(session, {})"),
            ("pymysql (sync)", "insert_query(session, {})"),
            ("mysqldb (sync)", "insert_query(session, {})"),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    Python::run(
                        py,
                        &std::ffi::CString::new(format!(
                            "session, engine = create_session('{name}')"
                        ))
                        .unwrap(),
                        None,
                        None,
                    )
                    .unwrap();

                    b.iter_custom(|iters| {
                        let mut sum = std::time::Duration::ZERO;
                        for g in 0..((iters - 1) / 10000 + 1) {
                            clear_table();
                            let start = g * 10000;
                            let end = iters.min(start + 10000);

                            let statement = stmt_template.replace("{}", &(end - start).to_string());
                            let c_statement = std::ffi::CString::new(statement).unwrap();

                            let start = std::time::Instant::now();
                            py.eval(c_statement.as_c_str(), None, None).unwrap();
                            sum += start.elapsed();
                        }
                        sum
                    });

                    Python::run(py, c"session.close(); engine.dispose()", None, None).unwrap();
                });
            });
        }
    }
}

criterion_group!(benches, bench_sqlalchemy);
criterion_main!(benches);
