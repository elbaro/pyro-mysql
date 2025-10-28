{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  PYO3_PYTHON = "python";

  buildInputs = [
    # Python with packages
    (pkgs.python3.withPackages (
      ps: with ps; [
        # Runtime dependencies
        greenlet
        sqlalchemy

        # Development dependencies
        pip
        pytest
        pytest-asyncio
        pytest-benchmark
        rich

        # Comparisons
        asyncmy
        aiomysql
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
