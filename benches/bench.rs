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

    for select_size in [1, 10, 100, 1000] {
        let mut group = c.benchmark_group(format!("SELECT_{}", select_size));

        for (name, setup, statement) in [
            (
                "mysqlclient",
                cr"mysqldb_conn = MySQLdb.connect(host='127.0.0.1', port=3306, user='test', password='1234', database='test', autocommit=True)",
                c"select_sync(mysqldb_conn)",
            ),
            (
                "pymysql",
                cr"pymysql_conn = pymysql.connect(host='127.0.0.1', port=3306, user='test', password='1234', database='test', autocommit=True)",
                c"select_sync(pymysql_conn)",
            ),
            (
                "pyro-sync",
                cr"pyro_sync_conn = pyro_mysql.SyncConn('mysql://test:1234@127.0.0.1:3306/test')",
                c"select_pyro_sync(pyro_sync_conn)",
            ),
            (
                "pyro-async",
                cr"pyro_async_conn = loop.run_until_complete(create_pyro_async_conn())",
                c"loop.run_until_complete(select_pyro_async(pyro_async_conn))",
            ),
            (
                "pyro-wtx",
                cr"pyro_wtx_conn = loop.run_until_complete(create_pyro_wtx_conn())",
                c"loop.run_until_complete(select_pyro_wtx(pyro_wtx_conn))",
            ),
            (
                "asyncmy",
                cr"asyncmy_conn = loop.run_until_complete(create_asyncmy_conn())",
                c"loop.run_until_complete(select_async(asyncmy_conn))",
            ),
            (
                "aiomysql",
                cr"aiomysql_conn = loop.run_until_complete(create_aiomysql_conn())",
                c"loop.run_until_complete(select_async(aiomysql_conn))",
            ),
        ] {
            group.bench_function(name, |b| {
                b.iter_batched(
                    || {
                        populate_table(select_size);                    
                        Python::attach(|py| {
                            Python::run(py, setup, None, None).unwrap();
                        });
                    },
                    |()| {
                        Python::attach(|py| {
                            py.run(&statement, None, None).unwrap();
                        });
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }
    {
        let mut group = c.benchmark_group("INSERT");

        for (name, setup, statement) in [
            (
                "mysqlclient",
                cr"mysqldb_conn = MySQLdb.connect(host='127.0.0.1', port=3306, user='test', password='1234', database='test', autocommit=True)",
                c"insert_sync(mysqldb_conn, 100)",
            ),
            (
                "pymysql",
                cr"pymysql_conn = pymysql.connect(host='127.0.0.1', port=3306, user='test', password='1234', database='test', autocommit=True)",
                c"insert_sync(pymysql_conn, 100)",
            ),
            (
                "pyro-sync",
                cr"pyro_sync_conn = pyro_mysql.SyncConn('mysql://test:1234@127.0.0.1:3306/test')",
                c"insert_pyro_sync(pyro_sync_conn, 100)",
            ),
            (
                "pyro-async",
                cr"pyro_async_conn = loop.run_until_complete(create_pyro_async_conn())",
                c"loop.run_until_complete(insert_pyro_async(pyro_async_conn, 100))",
            ),
            (
                "pyro-wtx",
                cr"pyro_wtx_conn = loop.run_until_complete(create_pyro_wtx_conn())",
                c"loop.run_until_complete(insert_pyro_wtx(pyro_wtx_conn, 100))",
            ),
            (
                "asyncmy",
                cr"asyncmy_conn = loop.run_until_complete(create_asyncmy_conn())",
                c"loop.run_until_complete(insert_async(asyncmy_conn, 100))",
            ),
            (
                "aiomysql",
                cr"aiomysql_conn = loop.run_until_complete(create_aiomysql_conn())",
                c"loop.run_until_complete(insert_async(aiomysql_conn, 100))",
            ),
        ] {
            group.bench_function(name, |b| {
                b.iter_batched(
                    || {
                        clear_table();
                        Python::attach(|py| {
                            Python::run(py, setup, None, None).unwrap();
                        });
                    },
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
