source("R/00_config.R")

schertz_discover_city_hub <- function(cfg = schertz_config) {
  schertz_check_packages(schertz_required_pipeline_packages)
  schertz_ensure_dirs(cfg)

  page <- schertz_read_html(cfg$hub_url, cfg = cfg)
  links <- schertz_extract_links(page, cfg$hub_url)

  out <- links |>
    dplyr::mutate(
      link_text_lower = stringr::str_to_lower(.data$link_text),
      absolute_url_lower = stringr::str_to_lower(.data$absolute_url)
    ) |>
    dplyr::filter(
      stringr::str_detect(.data$link_text_lower, "agenda|minutes|current|past|council") |
        stringr::str_detect(.data$absolute_url_lower, "destinyhosted|laserfiche|agenda|minutes")
    ) |>
    dplyr::select(link_text, absolute_url)

  saveRDS(out, file.path(cfg$derived_dir, "city_hub_links.rds"))
  readr::write_csv(out, file.path(cfg$derived_dir, "city_hub_links.csv"))
  out
}
