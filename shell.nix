{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = [
    # Python with packages
    (pkgs.python313.withPackages (
      ps: with ps; [
        # Runtime dependencies
        greenlet
        sqlalchemy
        typing-extensions

        # Development dependencies
        # pip
        pytest
        pytest-asyncio
        pytest-benchmark # broken in 3.14
        rich # broken in 3.14

        # Comparisons
        asyncmy # broken in 3.14
        aiomysql # broken in 3.14
        pymysql # broken in 3.14
        mysqlclient
      ]
    ))

    # Tools available as top-level packages
    pkgs.maturin
    pkgs.pyright

    # MySQL client library for diesel backend
    pkgs.libmysqlclient
  ];

  shellHook = ''
    export PYTHONPATH=.
    echo "pyro-mysql development environment"
    echo "Python version: $(python --version)"
  '';
}
