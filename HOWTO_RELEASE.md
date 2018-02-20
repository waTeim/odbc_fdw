# How to release

## Release procedure
1. Make sure tests are passing in [travis](https://travis-ci.org/CartoDB/odbc_fdw). Fix if broken before proceeding.
1. Ensure there's a proper `default_version` set in the [odbc_fdw.control](https://github.com/CartoDB/odbc_fdw/blob/master/odbc_fdw.control) file.
1. Ensure [NEWS.md](https://github.com/CartoDB/odbc_fdw/blob/master/NEWS.md) section exists for the new version, review it, update the release notes and date.
1. Create the required [extension sql files](https://www.postgresql.org/docs/10/static/extend-extensions.html#id-1.8.3.18.11). There must be new `odbc_fdw--next.sql`, `odbc_fdw--previous--next.sql` and `odbc_fdw--next--previous.sql` to be able to install it from scratch, upgrade and downgrade it.
1. Update the [Makefile](https://github.com/CartoDB/odbc_fdw/blob/master/Makefile) with the extension sql files (`DATA` section).
1. Commit `odbc_fdw.control`, `NEWS.md`, `Makefile` and the extension sql files.
1. git tag -a Major.Minor.Patch # use NEWS.md section as content.


## Version numbering convention

We try to stick to [semantic versioning v2.0.0](https://semver.org/spec/v2.0.0.html):

> Given a version number MAJOR.MINOR.PATCH, increment the:
>
> 1. MAJOR version when you make incompatible API changes,
> 2. MINOR version when you add functionality in a backwards-compatible manner, and
> 3. PATCH version when you make backwards-compatible bug fixes.
>
> Additional labels for pre-release and build metadata are available as extensions to the MAJOR.MINOR.PATCH format.
