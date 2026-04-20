schertz_config <- list(
  project_name = "schertz_council_docs_r2_app",
  meeting_body = "City Council",
  user_agent = "Mozilla/5.0 (compatible; SchertzCouncilR2App/1.0)",
  hub_url = "https://schertz.com/273/Agendas-Minutes",
  current_source_url = "https://public.destinyhosted.com/agenda_publish.cfm?id=72437",
  archive_source_url = "https://laserfiche.schertzweb.com/WebLink/Browse.aspx?id=54081&dbid=1&repo=SCHERTZ&cr=1",

  data_dir = "data",
  derived_dir = file.path("data", "derived"),

  local_cache_dir = "tmp",
  local_stage_raw_dir = file.path("tmp", "raw_stage"),
  logs_dir = "logs",

  max_downloads = Inf,
  request_seconds = 1800,
  sleep_seconds = 0.15,
  large_file_threshold_bytes = 50 * 1024^2
)

schertz_required_pipeline_packages <- c(
  "httr2", "rvest", "xml2", "dplyr", "purrr", "readr",
  "stringr", "tibble", "tidyr", "lubridate", "pdftools",
  "aws.s3"
)

schertz_required_app_packages <- c(
  "shiny", "bslib", "DT", "dplyr", "stringr", "readr",
  "tibble", "lubridate"
)

schertz_check_packages <- function(pkgs) {
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    stop(
      "Missing required packages: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }
}

schertz_ensure_dirs <- function(cfg = schertz_config) {
  dirs <- c(
    cfg$data_dir,
    cfg$derived_dir,
    cfg$local_cache_dir,
    cfg$local_stage_raw_dir,
    cfg$logs_dir
  )

  purrr::walk(
    dirs,
    ~ if (!dir.exists(.x)) dir.create(.x, recursive = TRUE, showWarnings = FALSE)
  )

  invisible(TRUE)
}

schertz_now <- function() {
  Sys.time()
}

schertz_guess_fiscal_year <- function(meeting_date) {
  x <- as.Date(meeting_date)
  out <- rep(NA_integer_, length(x))
  ok <- !is.na(x)
  out[ok] <- ifelse(
    as.integer(format(x[ok], "%m")) >= 10L,
    as.integer(format(x[ok], "%Y")) + 1L,
    as.integer(format(x[ok], "%Y"))
  )
  out
}

schertz_make_doc_id <- function(source_system, source_record_id, doc_type) {
  paste(source_system, source_record_id, doc_type, sep = "__")
}

schertz_read_html <- function(url, cfg = schertz_config) {
  resp <- httr2::request(url) |>
    httr2::req_user_agent(cfg$user_agent) |>
    httr2::req_timeout(cfg$request_seconds) |>
    httr2::req_perform()

  xml2::read_html(httr2::resp_body_string(resp))
}

schertz_download_binary <- function(url, destfile, cfg = schertz_config) {
  dir.create(dirname(destfile), recursive = TRUE, showWarnings = FALSE)

  curl::curl_download(
    url = url,
    destfile = destfile,
    mode = "wb",
    handle = curl::new_handle(
      useragent = cfg$user_agent,
      timeout = cfg$request_seconds
    )
  )

  invisible(destfile)
}

schertz_extract_links <- function(page, base_url) {
  hrefs <- rvest::html_elements(page, "a") |> rvest::html_attr("href")
  texts <- rvest::html_elements(page, "a") |> rvest::html_text2()

  tibble::tibble(
    href = hrefs,
    link_text = texts
  ) |>
    dplyr::filter(!is.na(.data$href), .data$href != "") |>
    dplyr::mutate(
      link_text = stringr::str_squish(.data$link_text),
      absolute_url = xml2::url_absolute(.data$href, base = base_url)
    ) |>
    dplyr::distinct(.data$absolute_url, .keep_all = TRUE)
}
