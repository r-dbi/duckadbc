#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbSendQueryArrow
#' @inheritParams DBI::dbBind
#' @param arrow Whether the query should be returned as an Arrow Table
#' @usage NULL
dbSendQueryArrow__duckdb_connection <- function(conn, statement, params = NULL, ...) {
  stopifnot(is.null(params))

  if (conn@debug) {
    message("Q ", statement)
  }
  statement <- enc2utf8(statement)
  stmt_lst <- rapi_prepare(conn@conn_ref, statement)

  res <- duckdb_result_arrow(
    connection = conn,
    stmt_lst = stmt_lst
  )

  return(res)
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbSendQueryArrow", "duckdb_connection", dbSendQueryArrow__duckdb_connection)
