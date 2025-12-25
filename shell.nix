{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    gnuplot
    openssl
    pkg-config

    # Python with packages
    (python3.withPackages (
      ps: with ps; [
        # Runtime dependencies
        greenlet
        sqlalchemy
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
  '';
}
