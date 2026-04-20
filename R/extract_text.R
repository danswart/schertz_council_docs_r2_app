source("R/00_config.R")
source("R/r2_storage.R")

schertz_extract_pdf_text <- function(path) {
  txt <- pdftools::pdf_text(path)
  txt <- stringr::str_squish(txt)
  list(
    text = paste(txt, collapse = "\n\n"),
    pages = length(txt)
  )
}

schertz_add_text_to_documents <- function(docs, cfg = schertz_config, only_missing = TRUE) {
  schertz_check_packages(schertz_required_pipeline_packages)
  schertz_ensure_dirs(cfg)

  if (nrow(docs) == 0) {
    return(docs)
  }

  if (!"r2_key" %in% names(docs)) docs$r2_key <- ""
  if (!"r2_present" %in% names(docs)) docs$r2_present <- FALSE

  for (i in seq_len(nrow(docs))) {
    if (isTRUE(only_missing) && isTRUE(docs$text_available[i])) {
      next
    }

    local_file <- docs$local_file[i]
    use_file <- local_file

    local_ok <- !is.na(local_file) && nzchar(local_file) && file.exists(local_file)

    if (!local_ok) {
      r2_key <- docs$r2_key[i]

      if (!is.na(r2_key) && nzchar(r2_key) && isTRUE(docs$r2_present[i])) {
        staged_name <- basename(r2_key)
        staged_path <- file.path(cfg$local_stage_raw_dir, staged_name)

        if (!file.exists(staged_path)) {
          message("Downloading from R2 for text extraction: ", docs$title[i])

          ok <- tryCatch({
            schertz_r2_download(r2_key, staged_path)
            TRUE
          }, error = function(e) {
            message("  R2 download failed: ", conditionMessage(e))
            FALSE
          })

          if (!isTRUE(ok)) {
            next
          }
        }

        use_file <- staged_path
        docs$local_file[i] <- staged_path
      } else {
        next
      }
    }

    message("Extracting text: ", basename(use_file))

    res <- tryCatch({
      schertz_extract_pdf_text(use_file)
    }, error = function(e) {
      message("  Text extraction failed: ", conditionMessage(e))
      NULL
    })

    if (!is.null(res)) {
      docs$text_extracted[i] <- res$text
      docs$text_available[i] <- nzchar(res$text)
      docs$n_pages[i] <- res$pages
      docs$text_extracted_at[i] <- schertz_now()
    }
  }

  saveRDS(docs, file.path(cfg$derived_dir, "documents_with_text.rds"))
  readr::write_csv(docs, file.path(cfg$derived_dir, "documents_with_text.csv"))
  docs
}
