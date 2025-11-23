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
        (sqlalchemy.overridePythonAttrs (old: rec {
          version = "2.0.44";
          src = pkgs.fetchPypi {
            pname = "sqlalchemy";
            inherit version;
            hash = "sha256-CudFThqx14Cu5p/SqufWuGcKWB2IR/LR4Pfd+/R+WiI=";
          };
        }))
        typing-extensions

        # Development dependencies
        pytest
        pytest-asyncio

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
