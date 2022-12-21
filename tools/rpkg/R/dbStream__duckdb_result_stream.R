#' @rdname duckdb_result-class
#' @inheritParams DBI::dbFetchArrow
#' @importFrom utils head
#' @usage NULL
dbFetchArrow__duckdb_result_arrow <- function(res, ..., chunk_size = 1000000) {
  if (!res@env$open) {
    stop("result set was closed")
  }

  duckdb_fetch_arrow(res, chunk_size)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbFetchArrow", "duckdb_result_arrow", dbFetchArrow__duckdb_result_arrow)
