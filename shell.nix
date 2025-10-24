{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = [
    # Python with packages
    (pkgs.python3.withPackages (
      ps: with ps; [
        # Runtime dependencies
        sqlalchemy

        # Development dependencies
        pip
        pytest-asyncio
        pytest-benchmark
        rich
        aiomysql
        greenlet
        pymysql
        mysqlclient
      ]
    ))

    # Tools available as top-level packages
    pkgs.maturin
    pkgs.pyright
  ];

  shellHook = ''
    echo "pyro-mysql development environment"
    echo "Python version: $(python --version)"
  '';
}
