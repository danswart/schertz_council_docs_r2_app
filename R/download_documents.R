source("R/00_config.R")
source("R/r2_storage.R")

schertz_local_filename <- function(doc) {
  ext <- tools::file_ext(doc$pdf_url)
  if (is.na(ext) || !nzchar(ext)) {
    ext <- "pdf"
  }

  paste0(gsub("[^A-Za-z0-9_-]", "_", doc$doc_id), ".", ext)
}

schertz_download_documents <- function(docs, cfg = schertz_config, only_missing = TRUE) {
  schertz_check_packages(schertz_required_pipeline_packages)
  schertz_ensure_dirs(cfg)

  if (nrow(docs) == 0) {
    return(docs)
  }

  if (!"r2_key" %in% names(docs)) docs$r2_key <- ""
  if (!"r2_present" %in% names(docs)) docs$r2_present <- FALSE

  for (i in seq_len(nrow(docs))) {
    if (!nzchar(docs$pdf_url[i])) {
      next
    }

    local_name <- schertz_local_filename(docs[i, ])
    destfile <- file.path(cfg$local_stage_raw_dir, local_name)

    if (is.na(docs$r2_key[i]) || !nzchar(docs$r2_key[i])) {
      docs$r2_key[i] <- schertz_r2_object_key(local_name)
    }

    r2_key <- docs$r2_key[i]
    in_r2 <- isTRUE(tryCatch(schertz_r2_exists(r2_key), error = function(e) FALSE))
    docs$r2_present[i] <- in_r2

    if (isTRUE(only_missing) && isTRUE(in_r2)) {
      next
    }

    if (
      isTRUE(only_missing) &&
      !in_r2 &&
      !is.na(docs$local_file[i]) &&
      nzchar(docs$local_file[i]) &&
      file.exists(docs$local_file[i])
    ) {
      message("Uploading existing local file to R2: ", docs$title[i])

      ok <- tryCatch({
        schertz_r2_put(docs$local_file[i], r2_key)
        TRUE
      }, error = function(e) {
        message("  R2 upload failed: ", conditionMessage(e))
        FALSE
      })

      if (isTRUE(ok)) {
        docs$r2_present[i] <- TRUE
        docs$downloaded_at[i] <- schertz_now()
      }

      next
    }

    message("Downloading: ", docs$title[i])

    ok <- tryCatch({
      schertz_download_binary(docs$pdf_url[i], destfile, cfg = cfg)
      schertz_r2_put(destfile, r2_key)
      TRUE
    }, error = function(e) {
      message("  Download/upload failed: ", conditionMessage(e))
      FALSE
    })

    if (isTRUE(ok)) {
      docs$local_file[i] <- destfile
      docs$r2_present[i] <- TRUE
      docs$downloaded_at[i] <- schertz_now()
    }

    Sys.sleep(cfg$sleep_seconds)
  }

  saveRDS(docs, file.path(cfg$derived_dir, "documents_downloaded.rds"))
  readr::write_csv(docs, file.path(cfg$derived_dir, "documents_downloaded.csv"))
  docs
}
