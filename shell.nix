{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    gnuplot
    openssl
    pkg-config

    # Python with packages
    (python313.withPackages (
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
        mariadb
      ]
    ))

    # Tools available as top-level packages
    maturin
    pyright
  ];

  shellHook = ''
    export PYTHONPATH=.
    echo "pyro-mysql development environment"
    echo "Python version: $(python --version)"
  '';
}
