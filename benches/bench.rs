use criterion::{Criterion, criterion_group, criterion_main};
use mysql::{TxOpts, prelude::Queryable};
use pyo3::{ffi::c_str, prelude::*};

fn setup_db() {
    let mut conn =
        mysql::Conn::new("mysql://test:1234@127.0.0.1:3306/test?prefer_socket=false").unwrap();
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
    let mut conn =
        mysql::Conn::new("mysql://test:1234@127.0.0.1:3306/test?prefer_socket=false").unwrap();
    conn.exec_drop("TRUNCATE TABLE benchmark_test", ()).unwrap();
}

fn populate_table(n: usize) {
    let mut conn =
        mysql::Conn::new("mysql://test:1234@127.0.0.1:3306/test?prefer_socket=false").unwrap();
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
        populate_table(select_size);

        for (name, setup, statement) in [
            (
                "mysqlclient (sync)",
                cr"mysqldb_conn = MySQLdb.connect(host='127.0.0.1', port=3306, user='test', password='1234', database='test', autocommit=True)",
                c"select_sync(mysqldb_conn)",
            ),
            (
                "pymysql (sync)",
                cr"pymysql_conn = pymysql.connect(host='127.0.0.1', port=3306, user='test', password='1234', database='test', autocommit=True)",
                c"select_sync(pymysql_conn)",
            ),
            (
                "pyro/mysql (sync)",
                cr"pyro_sync_conn = pyro_mysql.SyncConn('mysql://test:1234@127.0.0.1:3306/test?prefer_socket=false')",
                c"select_pyro_sync(pyro_sync_conn)",
            ),
            (
                "pyro/diesel (sync)",
                cr"pyro_diesel_conn = pyro_mysql.SyncConn('mysql://test:1234@127.0.0.1:3306/test', backend='diesel')",
                c"select_pyro_sync(pyro_diesel_conn)",
            ),
            (
                "pyro/zero (sync)",
                cr"pyro_zero_mysql_conn = pyro_mysql.SyncConn('mysql://test:1234@127.0.0.1:3306/test', backend='zero')",
                c"select_pyro_sync(pyro_zero_mysql_conn)",
            ),
            (
                "pyro/mysql (async)",
                cr"pyro_async_conn = loop.run_until_complete(create_pyro_async_conn())",
                c"loop.run_until_complete(select_pyro_async(pyro_async_conn))",
            ),
            (
                "pyro/wtx (async)",
                cr"pyro_wtx_conn = loop.run_until_complete(create_pyro_async_conn('wtx'))",
                c"loop.run_until_complete(select_pyro_async(pyro_wtx_conn))",
            ),
            (
                "pyro/zero (async)",
                cr"pyro_zero_mysql_async_conn = loop.run_until_complete(create_pyro_async_conn('zero'))",
                c"loop.run_until_complete(select_pyro_async(pyro_zero_mysql_async_conn))",
            ),
            (
                "asyncmy (async)",
                cr"asyncmy_conn = loop.run_until_complete(create_asyncmy_conn())",
                c"loop.run_until_complete(select_async(asyncmy_conn))",
            ),
            (
                "aiomysql (async)",
                cr"aiomysql_conn = loop.run_until_complete(create_aiomysql_conn())",
                c"loop.run_until_complete(select_async(aiomysql_conn))",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    Python::run(py, setup, None, None).unwrap();
                    b.iter(|| py.run(&statement, None, None).unwrap());
                });
            });
        }
    }
    {
        let mut group = c.benchmark_group("INSERT");

        for (name, setup, stmt_template) in [
            (
                "mysqlclient (sync)",
                cr"mysqldb_conn = MySQLdb.connect(host='127.0.0.1', port=3306, user='test', password='1234', database='test', autocommit=True)",
                "insert_sync(mysqldb_conn, {})",
            ),
            (
                "pymysql (sync)",
                cr"pymysql_conn = pymysql.connect(host='127.0.0.1', port=3306, user='test', password='1234', database='test', autocommit=True)",
                "insert_sync(pymysql_conn, {})",
            ),
            (
                "pyro/mysql (sync)",
                cr"pyro_sync_conn = pyro_mysql.SyncConn('mysql://test:1234@127.0.0.1:3306/test?prefer_socket=false')",
                "insert_pyro_sync(pyro_sync_conn, {})",
            ),
            (
                "pyro/diesel (sync)",
                cr"pyro_diesel_conn = pyro_mysql.SyncConn('mysql://test:1234@127.0.0.1:3306/test', backend='diesel')",
                "insert_pyro_sync(pyro_diesel_conn, {})",
            ),
            (
                "pyro/zero (sync)",
                cr"pyro_zero_mysql_conn = pyro_mysql.SyncConn('mysql://test:1234@127.0.0.1:3306/test', backend='zero')",
                "insert_pyro_sync(pyro_zero_mysql_conn, {})",
            ),
            (
                "pyro/mysql (async)",
                cr"pyro_async_conn = loop.run_until_complete(create_pyro_async_conn())",
                "loop.run_until_complete(insert_pyro_async(pyro_async_conn, {}))",
            ),
            (
                "pyro/wtx (async)",
                cr"pyro_wtx_conn = loop.run_until_complete(create_pyro_async_conn('wtx'))",
                "loop.run_until_complete(insert_pyro_async(pyro_wtx_conn, {}))",
            ),
            (
                "pyro/zero (async)",
                cr"pyro_zero_mysql_async_conn = loop.run_until_complete(create_pyro_async_conn('zero'))",
                "loop.run_until_complete(insert_pyro_async(pyro_zero_mysql_async_conn, {}))",
            ),
            (
                "asyncmy (async)",
                cr"asyncmy_conn = loop.run_until_complete(create_asyncmy_conn())",
                "loop.run_until_complete(insert_async(asyncmy_conn, {}))",
            ),
            (
                "aiomysql (async)",
                cr"aiomysql_conn = loop.run_until_complete(create_aiomysql_conn())",
                "loop.run_until_complete(insert_async(aiomysql_conn, {}))",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    Python::run(py, setup, None, None).unwrap();
                    b.iter_custom(|iters| {
                        let mut sum = std::time::Duration::ZERO;
                        for g in 0..((iters-1)/10000+1) {
                            clear_table();
                            let start = g * 10000;
                            let end = iters.min(start+10000);

                            let statement = stmt_template.replace("{}", &(end-start).to_string());
                            let c_statement = std::ffi::CString::new(statement).unwrap();

                            let start = std::time::Instant::now();
                            py.eval(c_statement.as_c_str(), None, None).unwrap();
                            sum += start.elapsed();

                            // Check no background tasks remain
                            py.run(c"assert len(__import__('asyncio').all_tasks(loop)) == 0", None, None).unwrap();
                        }
                        sum
                    });
                });
            });
        }
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
