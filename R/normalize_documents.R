source("R/00_config.R")

schertz_document_template <- function() {
  tibble::tibble(
    doc_id = character(),
    source_system = character(),
    source_category = character(),
    meeting_body = character(),
    meeting_type = character(),
    meeting_date = as.Date(character()),
    fiscal_year = integer(),
    doc_type = character(),
    title = character(),
    source_page_url = character(),
    document_url = character(),
    pdf_url = character(),
    source_record_id = character(),
    file_name = character(),
    local_file = character(),
    r2_key = character(),
    r2_present = logical(),
    text_extracted = character(),
    text_available = logical(),
    n_pages = integer(),
    discovered_at = as.POSIXct(character()),
    downloaded_at = as.POSIXct(character()),
    text_extracted_at = as.POSIXct(character()),
    is_active = logical(),
    notes = character()
  )
}

schertz_make_missing_column <- function(col_template, n) {
  if (inherits(col_template, "Date")) {
    return(as.Date(rep(NA_character_, n)))
  }

  if (inherits(col_template, "POSIXct")) {
    return(as.POSIXct(rep(NA_character_, n), origin = "1970-01-01", tz = "UTC"))
  }

  if (is.character(col_template)) {
    return(rep("", n))
  }

  if (is.integer(col_template)) {
    return(rep(NA_integer_, n))
  }

  if (is.logical(col_template)) {
    return(rep(FALSE, n))
  }

  if (is.double(col_template)) {
    return(rep(NA_real_, n))
  }

  rep(NA, n)
}

schertz_normalize_documents <- function(destiny_docs = NULL, laserfiche_docs = NULL, cfg = schertz_config) {
  pieces <- list(destiny_docs, laserfiche_docs)
  pieces <- pieces[!vapply(pieces, is.null, logical(1))]

  if (length(pieces) == 0) {
    out <- schertz_document_template()
    saveRDS(out, file.path(cfg$derived_dir, "documents_normalized.rds"))
    readr::write_csv(out, file.path(cfg$derived_dir, "documents_normalized.csv"))
    return(out)
  }

  docs <- dplyr::bind_rows(pieces)
  template <- schertz_document_template()

  missing_cols <- setdiff(names(template), names(docs))
  for (nm in missing_cols) {
    docs[[nm]] <- schertz_make_missing_column(template[[nm]], nrow(docs))
  }

  out <- docs |>
    dplyr::select(dplyr::all_of(names(template))) |>
    dplyr::mutate(
      title = dplyr::coalesce(.data$title, ""),
      doc_type = dplyr::coalesce(.data$doc_type, ""),
      meeting_type = dplyr::coalesce(.data$meeting_type, ""),
      meeting_body = dplyr::coalesce(.data$meeting_body, ""),
      source_system = dplyr::coalesce(.data$source_system, ""),
      source_category = dplyr::coalesce(.data$source_category, ""),
      source_page_url = dplyr::coalesce(.data$source_page_url, ""),
      document_url = dplyr::coalesce(.data$document_url, ""),
      pdf_url = dplyr::coalesce(.data$pdf_url, ""),
      source_record_id = dplyr::coalesce(.data$source_record_id, ""),
      file_name = dplyr::coalesce(.data$file_name, ""),
      local_file = dplyr::coalesce(.data$local_file, ""),
      r2_key = dplyr::coalesce(.data$r2_key, ""),
      r2_present = dplyr::coalesce(.data$r2_present, FALSE),
      text_extracted = dplyr::coalesce(.data$text_extracted, ""),
      text_available = dplyr::coalesce(.data$text_available, FALSE),
      is_active = dplyr::coalesce(.data$is_active, TRUE),
      notes = dplyr::coalesce(.data$notes, "")
    ) |>
    dplyr::distinct(.data$doc_id, .keep_all = TRUE) |>
    dplyr::arrange(dplyr::desc(.data$meeting_date), .data$title)

  saveRDS(out, file.path(cfg$derived_dir, "documents_normalized.rds"))
  readr::write_csv(out, file.path(cfg$derived_dir, "documents_normalized.csv"))
  out
}
