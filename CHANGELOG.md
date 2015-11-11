# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [1.6.0] - 2015-11-11
### Changed
- Massive reformatting and clean up of the entire code base.
- We now use Apache HTTP Client to make all of our HTTP requests.

### Added
- Added additional documentation.
- Added system properties configuration support.
- Added chained configuration context support.
- Added configuration context unit tests.

### Fixed
 - [HTTP Content-Type header is not being read correctly](https://github.com/joyent/java-manta/issues/39)
 - [README specifies incorrect system property name for manta user](https://github.com/joyent/java-manta/issues/48)
 - [manta client recursive delete does not work](https://github.com/joyent/java-manta/issues/49)
 - [manta client listObjects method does not completely populate manta objects](https://github.com/joyent/java-manta/issues/51)
 - [Convert MantaObject.getMtime() to use an actual Java time type](https://github.com/joyent/java-manta/issues/33)

## [1.5.4] - 2015-10-31
### Changed
- Project came under new leadership.
- Unit test framework changed to TestNG.
- MantaClient is no longer declared final.
- Changed developer information in pom.xml.
- More code formatting clean up and javadoc changes.

### Added
- Added MantaUtils test cases.
- Added additional key file path sanity checks.

## [1.5.3] - 2015-10-28
### Changed
- Migrated logger to use slf4j.
- Upgraded google-http-client to 1.19.0
- Moved Junit Maven scope to test.
- Upgraded JDK to 1.7 and replaced Commons IO method calls with standard library method calls.
- Upgraded bouncycastle libraries to 1.51.
- Maven now creates a shaded version that includes everything except bouncycastle and slf4j.
- Bumped Maven dependency versions.
- Enabled Manta unit tests to draw their configuration from environment variables.
- Updated release documentation.

### Added
- Added nexus release plugin.

### Fixed
- Fixed checkstyle failures.
- Fixed pom.xml settings so that it will properly release to Maven Central.