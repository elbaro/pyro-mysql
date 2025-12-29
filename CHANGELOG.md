# Changelog

## [0.2.14](https://github.com/elbaro/pyro-mysql/compare/v0.2.13...v0.2.14) (2025-12-29)


### Bug Fixes

* **ci:** specify AARCH64_UNKNOWN_LINUX_GNU_OPENSSL_* ([512b169](https://github.com/elbaro/pyro-mysql/commit/512b1696add7be7d9ac8586e2a036334215c4131))

## [0.2.13](https://github.com/elbaro/pyro-mysql/compare/v0.2.12...v0.2.13) (2025-12-29)


### Bug Fixes

* **ci:** apt-get update fails due to some urls ([d2f93dc](https://github.com/elbaro/pyro-mysql/commit/d2f93dc380e42b5b545bc0a23e62297b2a6ea684))

## [0.2.12](https://github.com/elbaro/pyro-mysql/compare/v0.2.11...v0.2.12) (2025-12-29)


### Bug Fixes

* **ci:** add ubuntu arm64 repos, point pkg-config to arm64 dir ([cf39e68](https://github.com/elbaro/pyro-mysql/commit/cf39e6810534bcadd0307b6049ae7574de978204))

## [0.2.11](https://github.com/elbaro/pyro-mysql/compare/v0.2.10...v0.2.11) (2025-12-29)


### Bug Fixes

* **ci:** set OPENSSL_DIR to cross-compilable openssl ([0d81831](https://github.com/elbaro/pyro-mysql/commit/0d81831aa6176d234c133f5b8961dcbefb4d4ec5))

## [0.2.10](https://github.com/elbaro/pyro-mysql/compare/v0.2.9...v0.2.10) (2025-12-29)


### Bug Fixes

* **ci:** add pkg-config ([db0118a](https://github.com/elbaro/pyro-mysql/commit/db0118a956be7ee492dbb43c55b4ff5f84ba0b0c))

## [0.2.9](https://github.com/elbaro/pyro-mysql/compare/v0.2.8...v0.2.9) (2025-12-29)


### Bug Fixes

* **ci:** build with python 3.14 ([d4efe23](https://github.com/elbaro/pyro-mysql/commit/d4efe236fdd4a9afbe575d1bf97278d4fc2e85d7))

## [0.2.8](https://github.com/elbaro/pyro-mysql/compare/v0.2.7...v0.2.8) (2025-12-29)


### Bug Fixes

* **ci:** use arm64 image for arm64 build, add windows arm64 ([3931202](https://github.com/elbaro/pyro-mysql/commit/393120250b9cd8d73c12349ff1e7a21109021082))

## [0.2.7](https://github.com/elbaro/pyro-mysql/compare/v0.2.6...v0.2.7) (2025-12-29)


### Bug Fixes

* **ci:** support both centos/debian manylinux images ([dcf03cc](https://github.com/elbaro/pyro-mysql/commit/dcf03cc462dc02042ac581c12978e1daa0e6edd1))

## [0.2.6](https://github.com/elbaro/pyro-mysql/compare/v0.2.5...v0.2.6) (2025-12-29)


### Bug Fixes

* **ci:** use debian, not centos ([a841707](https://github.com/elbaro/pyro-mysql/commit/a8417075800b617c1a2bb8bd0e00f32e6ff44809))

## [0.2.5](https://github.com/elbaro/pyro-mysql/compare/v0.2.4...v0.2.5) (2025-12-29)


### Bug Fixes

* use python 3.10 instead of 3.9 in windows ci ([5775b98](https://github.com/elbaro/pyro-mysql/commit/5775b986742516c0f1c2ac682b975a600fd406b9))

## [0.2.4](https://github.com/elbaro/pyro-mysql/compare/v0.2.3...v0.2.4) (2025-12-29)


### Bug Fixes

* trigger release ci ([89e4293](https://github.com/elbaro/pyro-mysql/commit/89e4293f59d458b0c38c02a8fb85b0f61182a275))

## [0.2.3](https://github.com/elbaro/pyro-mysql/compare/v0.2.2...v0.2.3) (2025-12-29)


### Features

* add MultiSyncConn ([ea550c5](https://github.com/elbaro/pyro-mysql/commit/ea550c58d5dc67ada8f97d9a31fa1fd536f16656))
* return tuple by default, and dict if as_dict=True ([2044b3d](https://github.com/elbaro/pyro-mysql/commit/2044b3dccb59a406782c58743a2e98c341aa3760))


### Bug Fixes

* async handlers store multiple column definitions correctly ([5cec72f](https://github.com/elbaro/pyro-mysql/commit/5cec72fae6258dd520f88d273db6ce34f74e23cb))
* **ci:** remove TEST_DATABASE_URL ([c772f65](https://github.com/elbaro/pyro-mysql/commit/c772f656fafb637b2c19c76f1422c349a5c09db2))


### Documentation

* book ([977a1e5](https://github.com/elbaro/pyro-mysql/commit/977a1e5ebb71ef0873a69916b2f2c00ea3127b4b))
* describe multi-backend in README.md ([9a88f57](https://github.com/elbaro/pyro-mysql/commit/9a88f570378595e8a3d7187e41e020d0f359e662))

## [0.2.2](https://github.com/elbaro/pyro-mysql/compare/pyro-mysql-v0.2.1...pyro-mysql-v0.2.2) (2025-12-29)


### Features

* add MultiSyncConn ([ea550c5](https://github.com/elbaro/pyro-mysql/commit/ea550c58d5dc67ada8f97d9a31fa1fd536f16656))
* return tuple by default, and dict if as_dict=True ([2044b3d](https://github.com/elbaro/pyro-mysql/commit/2044b3dccb59a406782c58743a2e98c341aa3760))


### Bug Fixes

* async handlers store multiple column definitions correctly ([5cec72f](https://github.com/elbaro/pyro-mysql/commit/5cec72fae6258dd520f88d273db6ce34f74e23cb))
* **ci:** remove TEST_DATABASE_URL ([c772f65](https://github.com/elbaro/pyro-mysql/commit/c772f656fafb637b2c19c76f1422c349a5c09db2))


### Documentation

* book ([977a1e5](https://github.com/elbaro/pyro-mysql/commit/977a1e5ebb71ef0873a69916b2f2c00ea3127b4b))
* describe multi-backend in README.md ([9a88f57](https://github.com/elbaro/pyro-mysql/commit/9a88f570378595e8a3d7187e41e020d0f359e662))
