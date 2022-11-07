#' @rdname duckdb_result-class
#' @inheritParams DBI::dbStream
#' @importFrom utils head
#' @usage NULL
dbStream__duckdb_result <- function(res, n = -1, ...) {
  if (!res@env$open) {
    stop("result set was closed")
  }

  stopifnot(res@arrow)

  if (n != -1) {
    stop("Cannot dbStream() an Arrow result unless n = -1")
  }

  duckdb_fetch_arrow(res)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbStream", "duckdb_result", dbStream__duckdb_result)
