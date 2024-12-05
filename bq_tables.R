
source_bq_tbl <- function(connection, schema, table, dataset = "ft-data") {
  #' Points to a data source in BQ
  #'
  #' Function pointing to a schema and table on BQ
  #'
  #' @param connection a database connection object
  #' @param schema string indicating the schema to reference e.g. datascience
  #' @param table string indicating the table to reference e.g. b2b_ltv
  #' @param dataset string indicating the dataset to reference e.g. ft-data
  #' @return tbl referencing the data source
  #' @export

  assertthat::assert_that(valid_bq_name(dataset),
                          valid_bq_name(schema),
                          valid_bq_name(table))
  dplyr::tbl(connection, I(paste0(dataset, ".", schema, ".", table)))
}

#' @export
source_tbl.BigQueryConnection <- function(connection, schema, table, dataset = "ft-data") {
  return(source_bq_tbl(connection, schema, table, dataset))
}

#' BigQuery table helpers
#'
#' Use these functions to access common tables in BigQuery. If you don't see the
#' table you need in this list use \code{\link{source_bq_tbl}}.
#' @param connection A BigQuery connection. Use \code{\link{ftdata_connection}}
#' @name bq_tables
NULL

#' @rdname bq_tables
#' @export
articles.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "articles", "articles_v2")

#' @rdname bq_tables
#' @export
user_status.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "dwpresentation", "fact_userstatus")

#' @rdname bq_tables
#' @export
fx_rates.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "dwpresentation", "dn_currencyexchangerate")

#' @rdname bq_tables
#' @export
rfv_daily.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "dwpresentation", "fact_usagerfv_daily")

#' @rdname bq_tables
#' @export
rfv_weekly.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "dwpresentation", "fact_usagerfv_weekly")

#' @rdname bq_tables
#' @export
arrangements.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "dwpresentation", "vw_nopii_dn_arrangement_all")

#' @rdname bq_tables
#' @export
arrangement_events.BigQueryConnection <- function(connection)
  source_bq_tbl(connection,
                "dwpresentation",
                "vw_nopii_dn_arrangementevent_all")

#' @rdname bq_tables
#' @export
org_arrangements.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "dwpresentation", "vw_nopii_dn_orgarrangement_all")

#' @rdname bq_tables
#' @export
org_arrangement_events.BigQueryConnection <- function(connection)
  source_bq_tbl(connection,
                "dwpresentation",
                "vw_nopii_dn_orgarrangementevent_all")

#' @rdname bq_tables
#' @export
clicks.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "spoor", "clicks")

#' @rdname bq_tables
#' @export
events.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "spoor", "events")

#' @rdname bq_tables
#' @export
pages.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "spoor", "pages")

#' @rdname bq_tables
#' @export
visitors.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "spoor", "visitors")

#' @rdname bq_tables
#' @export
visits.BigQueryConnection <- function(connection)
  source_bq_tbl(connection, "spoor", "visits")

#' @rdname bq_tables
#' @export
myft_follows.BigQueryConnection <- function(connection) {
  source_bq_tbl(connection,
                "dwpresentation",
                "vw_nopii_dn_preference") %>%
    dplyr::filter(.data$preference_status == "Enabled",
                  .data$preferencecategory_dkey == 7) %>%
    dplyr::select("preference_id",
                  "ft_user_id",
                  "concept_id",
                  "concorded_concept_id",
                  "name",
                  "concorded_name",
                  "type",
                  "description",
                  "original_follow_dtm",
                  "instant_alert_status",
                  "preference_created_by")
}
