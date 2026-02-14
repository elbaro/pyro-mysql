# Changelog

## [0.2.17](https://github.com/elbaro/pyro-mysql/compare/v0.2.16...v0.2.17) (2026-02-14)


### Bug Fixes

* cleanup CHANGELOG ([59f10cf](https://github.com/elbaro/pyro-mysql/commit/59f10cfd8ae69e7b9321e5c09e9303341dcbca7d))


### Documentation

* redraw benchmarks as svg ([724238b](https://github.com/elbaro/pyro-mysql/commit/724238b083485f85d9fbb2affffbe8940791b0bd))
* update README.md ([8b63c3c](https://github.com/elbaro/pyro-mysql/commit/8b63c3c00353293d34f81e77e1400917590e8007))

## [0.2.16](https://github.com/elbaro/pyro-mysql/compare/v0.2.2...v0.2.16) (2025-12-30)

### Bug Fixes

- update zero-mysql to ignore unix socket in windows ([1747674](https://github.com/elbaro/pyro-mysql/commit/174767423027fbc358bd63625b178b317defeaf2))
- **ci:** ubuntu arm64 CFLAGS ([f95dd87](https://github.com/elbaro/pyro-mysql/commit/f95dd8740aea8e348b0ad5ea9bc4caddcf6f862e))
- **ci:** specify AARCH64*UNKNOWN_LINUX_GNU_OPENSSL*\* ([512b169](https://github.com/elbaro/pyro-mysql/commit/512b1696add7be7d9ac8586e2a036334215c4131))
- **ci:** apt-get update fails due to some urls ([d2f93dc](https://github.com/elbaro/pyro-mysql/commit/d2f93dc380e42b5b545bc0a23e62297b2a6ea684))
- **ci:** add ubuntu arm64 repos, point pkg-config to arm64 dir ([cf39e68](https://github.com/elbaro/pyro-mysql/commit/cf39e6810534bcadd0307b6049ae7574de978204))
- **ci:** set OPENSSL_DIR to cross-compilable openssl ([0d81831](https://github.com/elbaro/pyro-mysql/commit/0d81831aa6176d234c133f5b8961dcbefb4d4ec5))
- **ci:** add pkg-config ([db0118a](https://github.com/elbaro/pyro-mysql/commit/db0118a956be7ee492dbb43c55b4ff5f84ba0b0c))
- **ci:** build with python 3.14 ([d4efe23](https://github.com/elbaro/pyro-mysql/commit/d4efe236fdd4a9afbe575d1bf97278d4fc2e85d7))
- **ci:** use arm64 image for arm64 build, add windows arm64 ([3931202](https://github.com/elbaro/pyro-mysql/commit/393120250b9cd8d73c12349ff1e7a21109021082))
- **ci:** support both centos/debian manylinux images ([dcf03cc](https://github.com/elbaro/pyro-mysql/commit/dcf03cc462dc02042ac581c12978e1daa0e6edd1))
- **ci:** use debian, not centos ([a841707](https://github.com/elbaro/pyro-mysql/commit/a8417075800b617c1a2bb8bd0e00f32e6ff44809))
- use python 3.10 instead of 3.9 in windows ci ([5775b98](https://github.com/elbaro/pyro-mysql/commit/5775b986742516c0f1c2ac682b975a600fd406b9))

## [0.2.2](https://github.com/elbaro/pyro-mysql/compare/pyro-mysql-v0.2.1...pyro-mysql-v0.2.2) (2025-12-29)

### Features

- return tuple by default, and dict if as_dict=True ([2044b3d](https://github.com/elbaro/pyro-mysql/commit/2044b3dccb59a406782c58743a2e98c341aa3760))

### Bug Fixes

- async handlers store multiple column definitions correctly ([5cec72f](https://github.com/elbaro/pyro-mysql/commit/5cec72fae6258dd520f88d273db6ce34f74e23cb))
- **ci:** remove TEST_DATABASE_URL ([c772f65](https://github.com/elbaro/pyro-mysql/commit/c772f656fafb637b2c19c76f1422c349a5c09db2))

### Documentation

- book ([977a1e5](https://github.com/elbaro/pyro-mysql/commit/977a1e5ebb71ef0873a69916b2f2c00ea3127b4b))
- describe multi-backend in README.md ([9a88f57](https://github.com/elbaro/pyro-mysql/commit/9a88f570378595e8a3d7187e41e020d0f359e662))
