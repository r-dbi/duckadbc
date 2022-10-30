skip_on_cran()
local_edition(3)

test_that("empty statement gives an error", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))
  expect_snapshot_error(dbGetQuery(con, "; ;   ; -- SELECT 1;"))
})

test_that("multiple statements can be used in one call", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))
  query <- paste(
    "CREATE TABLE integers(i integer);",
    "insert into integers select * from range(10);",
    "select * from integers;",
    sep = "\n"
  )
  expect_identical(dbGetQuery(con, query), data.frame(i = 0:9))
  expect_snapshot(dbGetQuery(con, paste("DROP TABLE IF EXISTS integers;", query)))
})

test_that("statements can be splitted apart correctly", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))
  expect_snapshot(dbGetQuery(con, a <- paste(
    "--Multistatement testing; testing",
    "/*  test;   ",
    "--test;",
    ";test */",
    "create table temp_test as ",
    "select",
    "'testing_temp;' as temp_col",
    ";",
    "select * from temp_test;",
    sep = "\n"
  )))
})

test_that("export/import database works", {
  export_location <- file.path(tempdir(), "duckdb_test_export")
  if (!file.exists(export_location)) dir.create(export_location)

  con <- dbConnect(duckdb())
  dbExecute(con, "CREATE TABLE integers(i integer)")
  dbExecute(con, "insert into integers select * from range(10)")
  dbExecute(con, "CREATE TABLE integers2(i INTEGER)")
  dbExecute(con, "INSERT INTO integers2 VALUES (1), (5), (7), (1928)")
  dbExecute(con, paste0("EXPORT DATABASE '", export_location, "'"))
  dbDisconnect(con, shutdown = TRUE)

  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("IMPORT DATABASE '", export_location, "'"))
  if (file.exists(export_location)) unlink(export_location, recursive = TRUE)

  expect_identical(dbGetQuery(con, "select * from integers"), data.frame(i = 0:9))
  expect_identical(dbGetQuery(con, "select * from integers2"), data.frame(i = c(1L, 5L, 7L, 1928L)))
})
