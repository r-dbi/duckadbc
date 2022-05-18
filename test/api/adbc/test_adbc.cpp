#include "catch.hpp"
#include "duckdb/common/adbc.hpp"

using namespace std;

TEST_CASE("Database init happy path", "[adbc]") {
	AdbcError adbc_error;
	AdbcStatusCode adbc_status;
	AdbcDatabaseOptions adbc_database_options;
	AdbcDatabase adbc_database;

	adbc_database_options.target = ":memory:";
	adbc_database_options.target_length = strlen(adbc_database_options.target);

	adbc_status = AdbcDatabaseInit(&adbc_database_options, &adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = AdbcDatabaseRelease(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// can release twice no problem
	adbc_status = AdbcDatabaseRelease(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// can release a nullptr
	adbc_status = AdbcDatabaseRelease(nullptr, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
}

TEST_CASE("Database init error conditions", "[adbc]") {
	AdbcError adbc_error;
	AdbcStatusCode adbc_status;
	AdbcDatabaseOptions adbc_database_options;
	AdbcDatabase adbc_database;

	// NULL options
	adbc_status = AdbcDatabaseInit(nullptr, &adbc_database, &adbc_error);
	REQUIRE(adbc_status != ADBC_STATUS_OK);
	REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
	AdbcErrorRelease(&adbc_error);

	// NULL database
	adbc_status = AdbcDatabaseInit(&adbc_database_options, nullptr, &adbc_error);
	REQUIRE(adbc_status != ADBC_STATUS_OK);
	REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
	AdbcErrorRelease(&adbc_error);

	// null error
	adbc_status = AdbcDatabaseInit(&adbc_database_options, nullptr, nullptr);
	REQUIRE(adbc_status != ADBC_STATUS_OK);

	// non-writeable path
	adbc_database_options.target = "/cant/write/this";
	adbc_database_options.target_length = strlen(adbc_database_options.target);
	adbc_status = AdbcDatabaseInit(&adbc_database_options, &adbc_database, &adbc_error);
	REQUIRE(adbc_status != ADBC_STATUS_OK);
	REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
	AdbcErrorRelease(&adbc_error);

	// also, we can release an error again
	AdbcErrorRelease(&adbc_error);
	// and we can release a nullptr
	AdbcErrorRelease(nullptr);
}
