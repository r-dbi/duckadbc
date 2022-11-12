#' @rdname duckdb_result-class
#' @inheritParams DBI::dbStream
#' @importFrom utils head
#' @usage NULL
dbStream__duckdb_result_stream <- function(res, ..., chunk_size = 1000000) {
  if (!res@env$open) {
    stop("result set was closed")
  }

  duckdb_fetch_arrow(res, chunk_size)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbStream", "duckdb_result_stream", dbStream__duckdb_result_stream)
