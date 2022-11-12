#' @rdname duckdb_result-class
#' @inheritParams DBI::dbClearResult
#' @usage NULL
dbClearResult__duckdb_result_stream <- function(res, ...) {
  dbClearResult__duckdb_result(res)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbClearResult", "duckdb_result_stream", dbClearResult__duckdb_result_stream)
