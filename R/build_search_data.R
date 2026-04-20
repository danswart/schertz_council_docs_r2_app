source("R/00_config.R")

schertz_build_search_data <- function(docs, cfg = schertz_config) {
  schertz_ensure_dirs(cfg)

  out <- docs |>
    dplyr::mutate(
      search_text = paste(
        dplyr::coalesce(.data$title, ""),
        dplyr::coalesce(.data$meeting_type, ""),
        dplyr::coalesce(.data$doc_type, ""),
        dplyr::coalesce(.data$text_extracted, "")
      ),
      link = dplyr::case_when(
        !is.na(.data$pdf_url) & nzchar(.data$pdf_url) ~ sprintf(
          "<a href='%s' target='_blank'>Open PDF</a>",
          .data$pdf_url
        ),
        !is.na(.data$document_url) & nzchar(.data$document_url) ~ sprintf(
          "<a href='%s' target='_blank'>Open Record</a>",
          .data$document_url
        ),
        TRUE ~ ""
      )
    ) |>
    dplyr::arrange(dplyr::desc(.data$meeting_date), .data$title)

  saveRDS(out, file.path(cfg$derived_dir, "documents.rds"))
  readr::write_csv(out, file.path(cfg$derived_dir, "documents.csv"))

  out
}
