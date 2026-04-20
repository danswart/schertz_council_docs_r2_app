source("R/00_config.R")
source("R/discover_city_hub.R")
source("R/discover_destinyhosted.R")
source("R/discover_laserfiche.R")
source("R/normalize_documents.R")
source("R/download_documents.R")
source("R/extract_text.R")
source("R/build_search_data.R")

schertz_build_index <- function(
  download_files = TRUE,
  extract_text = TRUE,
  include_archive = FALSE,
  start_year = 2019L,
  end_year = NULL,
  cfg = schertz_config
) {
  schertz_check_packages(schertz_required_pipeline_packages)
  schertz_ensure_dirs(cfg)

  message("Step 0: Reading Schertz hub page")
  invisible(try(schertz_discover_city_hub(cfg = cfg), silent = TRUE))

  message("Step 1: Discovering current Destiny Hosted documents")
  destiny_docs <- schertz_discover_destinyhosted(
    cfg = cfg,
    start_year = start_year,
    end_year = end_year
  )

  laserfiche_docs <- NULL
  if (isTRUE(include_archive)) {
    message("Step 2: Discovering Laserfiche archive documents")
    laserfiche_docs <- schertz_discover_laserfiche(cfg = cfg)
  }

  message("Step 3: Normalizing document metadata")
  docs <- schertz_normalize_documents(
    destiny_docs = destiny_docs,
    laserfiche_docs = laserfiche_docs,
    cfg = cfg
  )

  if (isTRUE(download_files)) {
    message("Step 4: Downloading PDFs")
    docs <- schertz_download_documents(docs, cfg = cfg, only_missing = TRUE)
  }

  if (isTRUE(extract_text)) {
    message("Step 5: Extracting PDF text")
    docs <- schertz_add_text_to_documents(docs, cfg = cfg, only_missing = TRUE)
  }

  message("Step 6: Building final search index")
  docs <- schertz_build_search_data(docs, cfg = cfg)

  message("Done.")
  docs
}
