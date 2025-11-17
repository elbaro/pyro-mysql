{
  pkgs ? import <nixpkgs> { },
}:

let
  # SQLAlchemy wheel from PyPI
  sqlalchemy-custom = pkgs.python3.pkgs.buildPythonPackage {
    pname = "SQLAlchemy";
    version = "2.0.44";
    format = "wheel";

    src = pkgs.fetchurl {
      url = "https://files.pythonhosted.org/packages/45/e5/5aa65852dadc24b7d8ae75b7efb8d19303ed6ac93482e60c44a585930ea5/sqlalchemy-2.0.44-cp312-cp312-manylinux_2_17_x86_64.manylinux2014_x86_64.whl";
      hash = "sha256-EZ3EHnp97878Vxic+g5hsb+cIoIRq6QytT+3HvNn/aE=";
    };

    doCheck = false;
  };
in
pkgs.mkShell {
  buildInputs = [
    # Python with packages
    (pkgs.python313.withPackages (
      ps: with ps; [
        # Runtime dependencies
        greenlet
        sqlalchemy-custom
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
