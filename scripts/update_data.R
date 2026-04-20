options(warn = 1)

message("=== Schertz weekly refresh started ===")
message("Start time: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))

project_root <- getwd()

if (!file.exists(file.path("R", "run_pipeline.R"))) {
  stop("Could not find R/run_pipeline.R in project root.")
}

source(file.path("R", "run_pipeline.R"))

schertz_check_packages(c(
  schertz_required_pipeline_packages,
  "jsonlite",
  "rsconnect"
))

schertz_ensure_dirs(schertz_config)

include_archive_refresh <- identical(
  tolower(Sys.getenv("SCHERTZ_INCLUDE_ARCHIVE", unset = "false")),
  "true"
)

end_year <- as.integer(format(Sys.Date(), "%Y"))
start_year <- suppressWarnings(as.integer(Sys.getenv("SCHERTZ_START_YEAR", unset = "2019")))
if (is.na(start_year)) {
  start_year <- 2019L
}

derived_dir <- schertz_config$derived_dir
rds_path <- file.path(derived_dir, "documents.rds")
csv_path <- file.path(derived_dir, "documents.csv")
meta_path <- file.path(derived_dir, "refresh_metadata.csv")
summary_path <- file.path(derived_dir, "refresh_comparison_summary.csv")
details_path <- file.path(derived_dir, "refresh_comparison_details.csv")

# -------------------------------------------------------------------
# 1. Read baseline before overwriting anything
# -------------------------------------------------------------------
baseline_docs <- NULL
if (file.exists(rds_path)) {
  message("Reading current documents.rds as baseline")
  baseline_docs <- readRDS(rds_path)
  baseline_docs <- tibble::as_tibble(baseline_docs)
} else {
  message("No prior documents.rds file found. Starting without baseline.")
  baseline_docs <- tibble::tibble()
}

# -------------------------------------------------------------------
# 2. Refresh current data for the requested window
# -------------------------------------------------------------------
fresh_docs <- schertz_build_index(
  download_files = TRUE,
  extract_text = TRUE,
  include_archive = include_archive_refresh,
  start_year = start_year,
  end_year = end_year,
  cfg = schertz_config
)

fresh_docs <- tibble::as_tibble(fresh_docs)

if (nrow(fresh_docs) == 0) {
  stop("Refresh failed: no rows returned from schertz_build_index().")
}

# -------------------------------------------------------------------
# 3. Merge refreshed window into baseline instead of replacing history
# -------------------------------------------------------------------
if (nrow(baseline_docs) == 0) {
  combined_docs <- fresh_docs
} else {
  if (!"doc_id" %in% names(baseline_docs)) baseline_docs$doc_id <- character()
  if (!"doc_id" %in% names(fresh_docs)) fresh_docs$doc_id <- character()

  fresh_ids <- unique(stats::na.omit(fresh_docs$doc_id))

  baseline_kept <- baseline_docs |>
    dplyr::filter(!(.data$doc_id %in% fresh_ids))

  combined_docs <- dplyr::bind_rows(fresh_docs, baseline_kept) |>
    dplyr::distinct(.data$doc_id, .keep_all = TRUE) |>
    dplyr::arrange(dplyr::desc(.data$meeting_date), .data$title)
}

# rebuild final search data and overwrite final outputs
combined_docs <- schertz_build_search_data(combined_docs, cfg = schertz_config)
new_docs <- tibble::as_tibble(combined_docs)

if (!file.exists(rds_path)) {
  stop("Refresh failed: documents.rds was not created.")
}

if (!file.exists(csv_path)) {
  stop("Refresh failed: documents.csv was not created.")
}

n_docs <- nrow(new_docs)
if (n_docs == 0) {
  stop("Refresh failed: documents dataset has 0 rows.")
}

if (!("meeting_date" %in% names(new_docs))) {
  stop("Refresh failed: meeting_date column is missing.")
}

recent_cutoff <- Sys.Date() - 400
n_recent <- sum(!is.na(new_docs$meeting_date) & new_docs$meeting_date >= recent_cutoff)

if (n_recent == 0) {
  stop("Refresh failed: no recent records found in the last 400 days.")
}

# -------------------------------------------------------------------
# 4. Compare baseline vs new output
# -------------------------------------------------------------------
old_docs <- tibble::as_tibble(baseline_docs)

if (!"doc_id" %in% names(old_docs)) old_docs$doc_id <- character()
if (!"doc_id" %in% names(new_docs)) new_docs$doc_id <- character()

old_ids <- unique(stats::na.omit(old_docs$doc_id))
new_ids <- unique(stats::na.omit(new_docs$doc_id))

only_in_old <- setdiff(old_ids, new_ids)
only_in_new <- setdiff(new_ids, old_ids)

removed_docs <- if (length(only_in_old) > 0) {
  old_docs |>
    dplyr::filter(.data$doc_id %in% only_in_old) |>
    dplyr::mutate(change_type = "removed")
} else {
  tibble::tibble()
}

added_docs <- if (length(only_in_new) > 0) {
  new_docs |>
    dplyr::filter(.data$doc_id %in% only_in_new) |>
    dplyr::mutate(change_type = "added")
} else {
  tibble::tibble()
}

comparison_details <- dplyr::bind_rows(added_docs, removed_docs)

if (nrow(comparison_details) > 0) {
  comparison_details <- comparison_details |>
    dplyr::select(
      "change_type",
      "doc_id",
      dplyr::any_of(c(
        "meeting_date", "fiscal_year", "source_system",
        "doc_type", "meeting_type", "title"
      ))
    ) |>
    dplyr::arrange(.data$change_type, dplyr::desc(.data$meeting_date), .data$title)
} else {
  comparison_details <- tibble::tibble(
    change_type = character(),
    doc_id = character(),
    meeting_date = as.Date(character()),
    fiscal_year = integer(),
    source_system = character(),
    doc_type = character(),
    meeting_type = character(),
    title = character()
  )
}

count_or_zero <- function(df, expr) {
  if (nrow(df) == 0) return(0L)
  sum(eval(substitute(expr), df, parent.frame()), na.rm = TRUE)
}

comparison_summary <- tibble::tibble(
  refreshed_at = Sys.time(),
  old_n = nrow(old_docs),
  new_n = nrow(new_docs),
  added_n = length(only_in_new),
  removed_n = length(only_in_old),
  old_destiny_n = count_or_zero(old_docs, source_system == "destinyhosted"),
  new_destiny_n = count_or_zero(new_docs, source_system == "destinyhosted"),
  old_laserfiche_n = count_or_zero(old_docs, source_system == "laserfiche"),
  new_laserfiche_n = count_or_zero(new_docs, source_system == "laserfiche"),
  old_minutes_n = count_or_zero(old_docs, doc_type == "minutes"),
  new_minutes_n = count_or_zero(new_docs, doc_type == "minutes"),
  old_agenda_packet_n = count_or_zero(old_docs, doc_type == "agenda_packet"),
  new_agenda_packet_n = count_or_zero(new_docs, doc_type == "agenda_packet")
)

readr::write_csv(comparison_summary, summary_path)
readr::write_csv(comparison_details, details_path)

# -------------------------------------------------------------------
# 5. Refresh metadata
# -------------------------------------------------------------------
refresh_meta <- tibble::tibble(
  refreshed_at = Sys.time(),
  documents_n = n_docs,
  recent_documents_n = n_recent,
  min_meeting_date = suppressWarnings(min(new_docs$meeting_date, na.rm = TRUE)),
  max_meeting_date = suppressWarnings(max(new_docs$meeting_date, na.rm = TRUE)),
  include_archive_refresh = include_archive_refresh,
  start_year = start_year,
  end_year = end_year
)

readr::write_csv(refresh_meta, meta_path)

# -------------------------------------------------------------------
# 6. Safety checks
# -------------------------------------------------------------------
if (nrow(old_docs) > 0) {
  old_total <- nrow(old_docs)
  new_total <- nrow(new_docs)

  if (new_total < floor(old_total * 0.95)) {
    stop(
      "Refresh failed safety check: new dataset dropped by more than 5%. ",
      "Old rows = ", old_total, "; New rows = ", new_total, "."
    )
  }
}

message("documents.rds: ", normalizePath(rds_path, winslash = "/", mustWork = FALSE))
message("documents.csv: ", normalizePath(csv_path, winslash = "/", mustWork = FALSE))
message("refresh_metadata.csv: ", normalizePath(meta_path, winslash = "/", mustWork = FALSE))
message("refresh_comparison_summary.csv: ", normalizePath(summary_path, winslash = "/", mustWork = FALSE))
message("refresh_comparison_details.csv: ", normalizePath(details_path, winslash = "/", mustWork = FALSE))
message("Rows in documents: ", n_docs)
message("Recent rows (last 400 days): ", n_recent)
message("Added doc_ids: ", length(only_in_new))
message("Removed doc_ids: ", length(only_in_old))
message("=== Schertz weekly refresh finished successfully ===")
