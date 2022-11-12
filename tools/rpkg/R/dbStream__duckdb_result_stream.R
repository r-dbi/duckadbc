#' @rdname duckdb_result-class
#' @inheritParams DBI::dbStream
#' @importFrom utils head
#' @usage NULL
dbStream__duckdb_result_stream <- function(res, n = -1, ...) {
  if (!res@env$open) {
    stop("result set was closed")
  }

  if (n != -1) {
    stop("Cannot dbStream() an Arrow result unless n = -1")
  }

  duckdb_fetch_arrow(res)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbStream", "duckdb_result_stream", dbStream__duckdb_result_stream)
