source("R/00_config.R")

schertz_destiny_listing_urls <- function(cfg = schertz_config, start_year = NULL, end_year = NULL, months_back = 18L) {
  today <- Sys.Date()

  if (is.null(end_year)) {
    end_year <- as.integer(format(today, "%Y"))
  }

  if (is.null(start_year)) {
    start_date <- seq(today, length = 2, by = paste0("-", months_back, " months"))[2]
    start_year <- as.integer(format(start_date, "%Y"))
  }

  month_seq <- seq(
    from = as.Date(sprintf("%s-01-01", start_year)),
    to   = as.Date(sprintf("%s-12-01", end_year)),
    by   = "month"
  )

  month_seq <- month_seq[month_seq <= as.Date(format(today, "%Y-%m-01"))]

  tibble::tibble(
    get_year = as.integer(format(month_seq, "%Y")),
    get_month = as.integer(format(month_seq, "%m"))
  ) |>
    dplyr::distinct(.data$get_year, .data$get_month) |>
    dplyr::mutate(
      listing_url = paste0(
        cfg$current_source_url,
        ifelse(grepl("\\?", cfg$current_source_url), "&", "?"),
        "get_month=", .data$get_month,
        "&get_year=", .data$get_year,
        "&mt=ALL"
      )
    )
}

schertz_extract_destiny_date <- function(x) {
  if (length(x) == 0 || is.na(x) || !nzchar(x)) {
    return(as.Date(NA))
  }

  x <- stringr::str_squish(x)

  candidates <- c(
    stringr::str_extract(x, "[A-Za-z]+\\s+\\d{1,2},\\s+\\d{4}"),
    stringr::str_extract(x, "\\d{2}-\\d{2}-\\d{2,4}"),
    stringr::str_extract(x, "\\d{8}")
  )
  candidates <- candidates[!is.na(candidates)]

  for (cand in candidates) {
    parsed <- suppressWarnings(lubridate::mdy(cand))
    if (!is.na(parsed)) return(as.Date(parsed))
    parsed <- suppressWarnings(lubridate::ymd(cand))
    if (!is.na(parsed)) return(as.Date(parsed))
  }

  as.Date(NA)
}

schertz_destiny_doc_type <- function(file_name, link_text = "") {
  target <- stringr::str_to_lower(paste(file_name, link_text))
  dplyr::case_when(
    stringr::str_detect(target, "minute") ~ "minutes",
    stringr::str_detect(target, "agenda") ~ "agenda_packet",
    TRUE ~ "other"
  )
}

schertz_destiny_meeting_type <- function(x) {
  target <- stringr::str_to_lower(dplyr::coalesce(x, ""))
  dplyr::case_when(
    stringr::str_detect(target, "pre-budget|budget") ~ "budget",
    stringr::str_detect(target, "workshop") ~ "workshop",
    stringr::str_detect(target, "special") ~ "special",
    stringr::str_detect(target, "regular") ~ "regular",
    TRUE ~ "other"
  )
}

schertz_extract_destiny_packet_links <- function(listing_url, cfg = schertz_config) {
  page <- tryCatch(schertz_read_html(listing_url, cfg = cfg), error = function(e) NULL)
  if (is.null(page)) {
    return(tibble::tibble())
  }

  links <- schertz_extract_links(page, listing_url)

  direct_docs <- links |>
    dplyr::filter(
      stringr::str_detect(.data$absolute_url, stringr::regex("scherdocs/.+\\.pdf($|\\?)", ignore_case = TRUE)),
      stringr::str_detect(.data$absolute_url, "/CC/")
    ) |>
    dplyr::mutate(source_page_url = listing_url)

  agenda_pages <- links |>
    dplyr::filter(
      stringr::str_detect(.data$absolute_url, stringr::fixed("agenda_publish.cfm")),
      stringr::str_detect(.data$absolute_url, stringr::fixed("dsp=ag"))
    ) |>
    dplyr::mutate(source_page_url = listing_url)

  packet_docs <- purrr::map_dfr(
    agenda_pages$absolute_url,
    function(detail_url) {
      Sys.sleep(cfg$sleep_seconds)
      detail_page <- tryCatch(schertz_read_html(detail_url, cfg = cfg), error = function(e) NULL)
      if (is.null(detail_page)) {
        return(tibble::tibble())
      }

      schertz_extract_links(detail_page, detail_url) |>
        dplyr::filter(
          stringr::str_detect(.data$absolute_url, stringr::regex("scherdocs/.+\\.pdf($|\\?)", ignore_case = TRUE)),
          stringr::str_detect(.data$absolute_url, "/CC/")
        ) |>
        dplyr::mutate(source_page_url = detail_url)
    }
  )

  dplyr::bind_rows(direct_docs, packet_docs) |>
    dplyr::distinct(.data$absolute_url, .keep_all = TRUE)
}

schertz_discover_destinyhosted <- function(cfg = schertz_config, start_year = NULL, end_year = NULL, months_back = 18L) {
  schertz_check_packages(schertz_required_pipeline_packages)
  schertz_ensure_dirs(cfg)

  listing_urls <- schertz_destiny_listing_urls(
    cfg = cfg,
    start_year = start_year,
    end_year = end_year,
    months_back = months_back
  )

  message("Scanning Destiny Hosted listing pages...")
  raw_links <- purrr::map_dfr(
    listing_urls$listing_url,
    function(u) {
      message("  ", u)
      Sys.sleep(cfg$sleep_seconds)
      schertz_extract_destiny_packet_links(u, cfg = cfg)
    }
  )

  if (nrow(raw_links) == 0) {
    return(tibble::tibble())
  }

  raw_links <- raw_links |>
    dplyr::mutate(
      file_name = basename(.data$absolute_url),
      parsed_date = purrr::pmap(
        list(.data$link_text, .data$absolute_url, .data$source_page_url, .data$file_name),
        function(link_text, absolute_url, source_page_url, file_name) {
          schertz_extract_destiny_date(
            paste(link_text, absolute_url, source_page_url, file_name)
          )
        }
      ) |>
        unlist() |>
        as.Date(origin = "1970-01-01")
    )

  saveRDS(raw_links, file.path(cfg$derived_dir, "destinyhosted_raw_links.rds"))
  readr::write_csv(raw_links, file.path(cfg$derived_dir, "destinyhosted_raw_links.csv"))

  out <- raw_links |>
    dplyr::transmute(
      doc_id = schertz_make_doc_id(
        "destinyhosted",
        dplyr::coalesce(
          stringr::str_match(.data$absolute_url, "/(\\d{8}_[0-9]+(?:/[^/]+)?[^/]*\\.pdf)$")[, 2],
          .data$file_name
        ),
        schertz_destiny_doc_type(.data$file_name, .data$link_text)
      ),
      source_system = "destinyhosted",
      source_category = "current",
      meeting_body = cfg$meeting_body,
      meeting_type = schertz_destiny_meeting_type(paste(.data$link_text, .data$file_name)),
      meeting_date = .data$parsed_date,
      fiscal_year = schertz_guess_fiscal_year(.data$parsed_date),
      doc_type = schertz_destiny_doc_type(.data$file_name, .data$link_text),
      title = dplyr::if_else(
        .data$link_text == "",
        stringr::str_replace(.data$file_name, "\\.pdf$", ""),
        .data$link_text
      ),
      source_page_url = .data$source_page_url,
      document_url = .data$absolute_url,
      pdf_url = .data$absolute_url,
      source_record_id = dplyr::coalesce(
        stringr::str_match(.data$absolute_url, "/(\\d{8}_[0-9]+(?:/[^/]+)?[^/]*\\.pdf)$")[, 2],
        .data$file_name
      ),
      file_name = .data$file_name,
      local_file = NA_character_,
      r2_key = NA_character_,
      r2_present = FALSE,
      text_extracted = NA_character_,
      text_available = FALSE,
      n_pages = NA_integer_,
      discovered_at = schertz_now(),
      downloaded_at = as.POSIXct(NA),
      text_extracted_at = as.POSIXct(NA),
      is_active = TRUE,
      notes = NA_character_
    ) |>
    dplyr::filter(.data$doc_type %in% c("agenda_packet", "minutes")) |>
    dplyr::distinct(.data$document_url, .keep_all = TRUE) |>
    dplyr::arrange(dplyr::desc(.data$meeting_date), .data$doc_type, .data$title)

  saveRDS(out, file.path(cfg$derived_dir, "destinyhosted_documents.rds"))
  readr::write_csv(out, file.path(cfg$derived_dir, "destinyhosted_documents.csv"))
  out
}
