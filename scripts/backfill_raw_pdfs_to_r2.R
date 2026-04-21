options(warn = 1)

source("R/00_config.R")
source("R/r2_storage.R")

schertz_check_packages(c(schertz_required_pipeline_packages, "aws.s3"))

project_root <- "/Users/D/R Working Directory/schertz_council_docs_r2_app"
source_dir <- "/Users/D/R Working Directory/schertz_shiny_phase_2_council_agendas_minutes_packets_app/deploy_app/data/raw"

batch_size <- 25L
start_index <- 1L

setwd(project_root)
schertz_ensure_dirs(schertz_config)

if (!dir.exists(source_dir)) {
  stop("Source directory does not exist: ", source_dir)
}

manifest_path <- file.path("data", "derived", "r2_backfill_manifest.csv")

if (file.exists(manifest_path)) {
  manifest <- readr::read_csv(manifest_path, show_col_types = FALSE)
} else {
  manifest <- tibble::tibble(
    local_file = character(),
    file_name = character(),
    object_key = character(),
    file_size_bytes = numeric(),
    uploaded = logical(),
    uploaded_at = as.POSIXct(character()),
    note = character()
  )
}

pdf_files <- list.files(
  source_dir,
  pattern = "\\.pdf$",
  full.names = TRUE,
  recursive = TRUE
)

if (length(pdf_files) == 0) {
  stop("No PDF files found in source directory: ", source_dir)
}

already_done <- manifest |>
  dplyr::filter(.data$uploaded) |>
  dplyr::pull(.data$local_file)

remaining_files <- setdiff(pdf_files, already_done)

if (length(remaining_files) == 0) {
  message("No remaining files to backfill.")
} else {
  batch_files <- remaining_files[start_index:min(length(remaining_files), start_index + batch_size - 1L)]

  message("=== R2 backfill batch started ===")
  message("Project root: ", project_root)
  message("Source dir: ", source_dir)
  message("Files found total: ", length(pdf_files))
  message("Already uploaded: ", length(already_done))
  message("This batch size: ", length(batch_files))

  results <- purrr::map_dfr(
    batch_files,
    function(path) {
      info <- file.info(path)
      file_name <- basename(path)
      object_key <- schertz_r2_object_key(file_name)

      message("Uploading: ", file_name)

      exists_now <- tryCatch(
        schertz_r2_exists(object_key),
        error = function(e) FALSE
      )

      if (isTRUE(exists_now)) {
        return(tibble::tibble(
          local_file = path,
          file_name = file_name,
          object_key = object_key,
          file_size_bytes = info$size,
          uploaded = TRUE,
          uploaded_at = Sys.time(),
          note = "Already existed in R2"
        ))
      }

      ok <- tryCatch(
        {
          schertz_r2_put(path, object_key, cfg = schertz_config)
          TRUE
        },
        error = function(e) {
          message("  Upload failed: ", conditionMessage(e))
          FALSE
        }
      )

      tibble::tibble(
        local_file = path,
        file_name = file_name,
        object_key = object_key,
        file_size_bytes = info$size,
        uploaded = ok,
        uploaded_at = if (ok) Sys.time() else as.POSIXct(NA),
        note = if (ok) "Uploaded" else "Upload failed"
      )
    }
  )

  manifest <- dplyr::bind_rows(manifest, results) |>
    dplyr::arrange(dplyr::desc(.data$uploaded_at), .data$file_name) |>
    dplyr::distinct(.data$local_file, .keep_all = TRUE)

  readr::write_csv(manifest, manifest_path)

  message("=== R2 backfill batch finished ===")
  message("Batch attempted: ", nrow(results))
  message("Batch uploaded/confirmed: ", sum(results$uploaded, na.rm = TRUE))
  message("Manifest: ", normalizePath(manifest_path, winslash = "/", mustWork = FALSE))
}
