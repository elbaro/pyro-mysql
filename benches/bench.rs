use criterion::{Criterion, criterion_group, criterion_main};
use pyo3::{ffi::c_str, prelude::*};

fn setup_db(py: Python) {
    py.run(
        c"pyro_setup_conn = pyro_mysql.SyncConn('mysql://test:1234@localhost:3306/test')",
        None,
        None,
    )
    .unwrap();
    py.run(
        c"pyro_setup_conn.query_drop('DROP TABLE IF EXISTS benchmark_test')",
        None,
        None,
    )
    .unwrap();
    py.run(
        c"pyro_setup_conn.query_drop('''CREATE TABLE benchmark_test (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100),
            age INT,
            email VARCHAR(100),
            score FLOAT,
            description VARCHAR(100)
        ) ENGINE = MEMORY''')",
        None,
        None,
    )
    .unwrap();
    py.run(c"pyro_setup_conn.close()", None, None).unwrap();
}

fn clear_table(py: Python) {
    py.run(
        c"pyro_clear_conn = pyro_mysql.SyncConn('mysql://test:1234@localhost:3306/test')",
        None,
        None,
    )
    .unwrap();
    py.run(
        c"pyro_clear_conn.query_drop('TRUNCATE TABLE benchmark_test')",
        None,
        None,
    )
    .unwrap();
    py.run(c"pyro_clear_conn.close()", None, None).unwrap();
}

fn populate_table(py: Python, n: usize) {
    py.run(
        c"pyro_pop_conn = pyro_mysql.SyncConn('mysql://test:1234@localhost:3306/test')",
        None,
        None,
    )
    .unwrap();
    py.run(
        c"pyro_pop_conn.query_drop('TRUNCATE TABLE benchmark_test')",
        None,
        None,
    )
    .unwrap();

    let insert_code = format!(
        r#"
for i in range({}):
    pyro_pop_conn.exec_drop(
        "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
        (f"user_{{i}}", 20 + (i % 50), f"user{{i}}@example.com", float(i % 100), f"User description {{i}}")
    )
"#,
        n
    );
    let c_insert_code = std::ffi::CString::new(insert_code).unwrap();
    py.run(c_insert_code.as_c_str(), None, None).unwrap();
    py.run(c"pyro_pop_conn.close()", None, None).unwrap();
}

pub fn bench(c: &mut Criterion) {
    Python::attach(|py| {
        Python::run(py, c_str!(include_str!("./bench.py")), None, None).unwrap();
        setup_db(py);
    });

    for select_size in [1, 10, 100, 1000] {
        let mut group = c.benchmark_group(format!("SELECT_{}", select_size));
        Python::attach(|py| populate_table(py, select_size));

        for (name, setup, statement) in [
            (
                "mysqlclient (sync)",
                cr"mysqldb_conn = MySQLdb.connect(host='localhost', port=3306, user='test', password='1234', database='test', autocommit=True)",
                c"select_sync(mysqldb_conn)",
            ),
            (
                "pymysql (sync)",
                cr"pymysql_conn = pymysql.connect(host='localhost', port=3306, user='test', password='1234', database='test', autocommit=True)",
                c"select_sync(pymysql_conn)",
            ),
            (
                "mariadb (sync)",
                cr"mariadb_conn = create_mariadb_conn()",
                c"select_mariadb(mariadb_conn)",
            ),
            (
                "pyro (sync)",
                cr"pyro_sync_conn = pyro_mysql.SyncConn('mysql://test:1234@localhost:3306/test')",
                c"select_pyro_sync(pyro_sync_conn)",
            ),
            (
                "pyro (async)",
                cr"pyro_async_conn = loop.run_until_complete(create_pyro_async_conn())",
                c"loop.run_until_complete(select_pyro_async(pyro_async_conn))",
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
                cr"mysqldb_conn = MySQLdb.connect(host='localhost', port=3306, user='test', password='1234', database='test', autocommit=True)",
                "insert_sync(mysqldb_conn, {})",
            ),
            (
                "pymysql (sync)",
                cr"pymysql_conn = pymysql.connect(host='localhost', port=3306, user='test', password='1234', database='test', autocommit=True)",
                "insert_sync(pymysql_conn, {})",
            ),
            (
                "mariadb (sync)",
                cr"mariadb_conn = create_mariadb_conn()",
                "insert_mariadb(mariadb_conn, {})",
            ),
            (
                "mariadb (sync, bulk)",
                cr"mariadb_bulk_conn = create_mariadb_conn()",
                "insert_mariadb_bulk(mariadb_bulk_conn, {})",
            ),
            (
                "pyro (sync)",
                cr"pyro_sync_conn = pyro_mysql.SyncConn('mysql://test:1234@localhost:3306/test')",
                "insert_pyro_sync(pyro_sync_conn, {})",
            ),
            (
                "pyro (sync, bulk)",
                cr"pyro_sync_bulk_conn = pyro_mysql.SyncConn('mysql://test:1234@localhost:3306/test')",
                "insert_pyro_sync_bulk(pyro_sync_bulk_conn, {})",
            ),
            (
                "pyro (async)",
                cr"pyro_async_conn = loop.run_until_complete(create_pyro_async_conn())",
                "loop.run_until_complete(insert_pyro_async(pyro_async_conn, {}))",
            ),
            (
                "pyro (async, bulk)",
                cr"pyro_async_bulk_conn = loop.run_until_complete(create_pyro_async_conn())",
                "loop.run_until_complete(insert_pyro_async_bulk(pyro_async_bulk_conn, {}))",
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
                            clear_table(py);
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
