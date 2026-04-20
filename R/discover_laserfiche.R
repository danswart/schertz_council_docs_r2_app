source("R/00_config.R")

schertz_discover_laserfiche <- function(cfg = schertz_config) {
  schertz_check_packages(schertz_required_pipeline_packages)
  schertz_ensure_dirs(cfg)

  message("Laserfiche discovery is scaffolded but intentionally conservative in Prototype 3.")
  message("Use current-system text extraction first, then extend archive crawling once the current pipeline is stable.")

  out <- tibble::tibble(
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
    text_extracted = character(),
    text_available = logical(),
    n_pages = integer(),
    discovered_at = as.POSIXct(character()),
    downloaded_at = as.POSIXct(character()),
    text_extracted_at = as.POSIXct(character()),
    is_active = logical(),
    notes = character()
  )

  saveRDS(out, file.path(cfg$derived_dir, "laserfiche_documents.rds"))
  readr::write_csv(out, file.path(cfg$derived_dir, "laserfiche_documents.csv"))
  out
}
