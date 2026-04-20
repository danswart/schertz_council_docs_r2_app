library(shiny)
library(bslib)
library(DT)
library(dplyr)
library(readr)
library(tibble)
library(lubridate)

source("R/00_config.R")

schertz_check_packages(schertz_required_app_packages)
schertz_ensure_dirs(schertz_config)

load_documents <- function(cfg = schertz_config) {
  rds_path <- file.path(cfg$derived_dir, "documents.rds")
  csv_path <- file.path(cfg$derived_dir, "documents.csv")

  if (file.exists(rds_path)) {
    docs <- readRDS(rds_path)
  } else if (file.exists(csv_path)) {
    docs <- readr::read_csv(csv_path, show_col_types = FALSE)
  } else {
    docs <- tibble::tibble(
      doc_id = character(),
      meeting_date = as.Date(character()),
      fiscal_year = integer(),
      doc_type = character(),
      meeting_type = character(),
      title = character(),
      text_extracted = character(),
      search_text = character(),
      link = character(),
      source_system = character()
    )
  }

  docs <- tibble::as_tibble(docs)

  if (!"meeting_date" %in% names(docs)) docs$meeting_date <- as.Date(character())
  if (!"fiscal_year" %in% names(docs)) docs$fiscal_year <- NA_integer_
  if (!"doc_type" %in% names(docs)) docs$doc_type <- ""
  if (!"meeting_type" %in% names(docs)) docs$meeting_type <- ""
  if (!"title" %in% names(docs)) docs$title <- ""
  if (!"text_extracted" %in% names(docs)) docs$text_extracted <- ""
  if (!"link" %in% names(docs)) docs$link <- ""
  if (!"source_system" %in% names(docs)) docs$source_system <- ""

  docs$search_text <- paste(
    dplyr::coalesce(as.character(docs$title), ""),
    dplyr::coalesce(as.character(docs$meeting_type), ""),
    dplyr::coalesce(as.character(docs$doc_type), ""),
    dplyr::coalesce(as.character(docs$text_extracted), "")
  )

  docs |>
    dplyr::mutate(
      meeting_date = suppressWarnings(as.Date(.data$meeting_date)),
      title = dplyr::coalesce(as.character(.data$title), ""),
      doc_type = dplyr::coalesce(as.character(.data$doc_type), ""),
      meeting_type = dplyr::coalesce(as.character(.data$meeting_type), ""),
      text_extracted = dplyr::coalesce(as.character(.data$text_extracted), ""),
      search_text = dplyr::coalesce(as.character(.data$search_text), ""),
      link = dplyr::coalesce(as.character(.data$link), ""),
      source_system = dplyr::coalesce(as.character(.data$source_system), "")
    ) |>
    dplyr::arrange(dplyr::desc(.data$meeting_date), .data$title)
}

load_refresh_metadata <- function(cfg = schertz_config) {
  meta_path <- file.path(cfg$derived_dir, "refresh_metadata.csv")

  if (!file.exists(meta_path)) {
    return(NULL)
  }

  meta <- tryCatch(
    {
      readr::read_csv(meta_path, show_col_types = FALSE)
    },
    error = function(e) {
      NULL
    }
  )

  if (is.null(meta) || nrow(meta) == 0) {
    return(NULL)
  }

  meta
}

format_refresh_time <- function(meta) {
  if (is.null(meta) || !"refreshed_at" %in% names(meta) || is.na(meta$refreshed_at[1])) {
    return("Unknown")
  }

  parsed <- suppressWarnings(as.POSIXct(meta$refreshed_at[1], tz = Sys.timezone()))
  if (is.na(parsed)) {
    return(as.character(meta$refreshed_at[1]))
  }

  format(parsed, "%B %d, %Y at %I:%M %p")
}

documents <- load_documents()
refresh_meta <- load_refresh_metadata()

safe_choices <- function(x) {
  vals <- sort(unique(x))
  vals <- vals[!is.na(vals) & vals != ""]
  c("All", vals)
}

min_date <- if (nrow(documents) > 0 && any(!is.na(documents$meeting_date))) {
  min(documents$meeting_date, na.rm = TRUE)
} else {
  Sys.Date() - 365
}

max_date <- if (nrow(documents) > 0 && any(!is.na(documents$meeting_date))) {
  max(documents$meeting_date, na.rm = TRUE)
} else {
  Sys.Date()
}

last_refreshed_text <- format_refresh_time(refresh_meta)

ui <- bslib::page_sidebar(
  title = "Schertz City Council Documents",
  theme = bslib::bs_theme(version = 5),
  sidebar = bslib::sidebar(
    width = 320,
    shiny::textInput(
      "search_text",
      "Search",
      placeholder = "Try budget, Westbrook, vote, minutes"
    ),
    shiny::selectInput(
      "doc_type",
      "Document type",
      choices = safe_choices(documents$doc_type),
      selected = "All"
    ),
    shiny::selectInput(
      "meeting_type",
      "Meeting type",
      choices = safe_choices(documents$meeting_type),
      selected = "All"
    ),
    shiny::selectInput(
      "fiscal_year",
      "Fiscal year",
      choices = c("All", sort(unique(stats::na.omit(documents$fiscal_year)))),
      selected = "All"
    ),
    shiny::dateRangeInput(
      "date_range",
      "Meeting date range",
      start = min_date,
      end = max_date,
      min = min_date,
      max = max_date,
      startview = "year"
    ),
    shiny::actionButton("clear_filters", "Clear filters"),
    tags$hr(),
    tags$p(
      style = "font-size: 0.9em; color: #555;",
      "This app reads local finished files only. Full-text search works where extracted text is available."
    ),
    tags$p(
      style = "font-size: 0.9em; color: #555; margin-bottom: 0;",
      tags$strong("Last refreshed: "),
      last_refreshed_text
    )
  ),
  bslib::card(
    bslib::card_body(
      shiny::htmlOutput("summary_text"),
      DT::DTOutput("documents_table")
    )
  )
)

server <- function(input, output, session) {
  shiny::observeEvent(input$clear_filters, {
    shiny::updateTextInput(session, "search_text", value = "")
    shiny::updateSelectInput(session, "doc_type", selected = "All")
    shiny::updateSelectInput(session, "meeting_type", selected = "All")
    shiny::updateSelectInput(session, "fiscal_year", selected = "All")
    shiny::updateDateRangeInput(session, "date_range", start = min_date, end = max_date)
  })

  filtered_docs <- shiny::reactive({
    docs <- documents

    if (nrow(docs) == 0) {
      return(docs)
    }

    if (!is.null(input$doc_type) && input$doc_type != "All") {
      docs <- dplyr::filter(docs, .data$doc_type == input$doc_type)
    }

    if (!is.null(input$meeting_type) && input$meeting_type != "All") {
      docs <- dplyr::filter(docs, .data$meeting_type == input$meeting_type)
    }

    if (!is.null(input$fiscal_year) && input$fiscal_year != "All") {
      docs <- dplyr::filter(docs, as.character(.data$fiscal_year) == as.character(input$fiscal_year))
    }

    if (!is.null(input$date_range) && length(input$date_range) == 2) {
      start_date <- as.Date(input$date_range[1])
      end_date <- as.Date(input$date_range[2])
      docs <- dplyr::filter(
        docs,
        is.na(.data$meeting_date) |
          (.data$meeting_date >= start_date & .data$meeting_date <= end_date)
      )
    }

    q <- trimws(if (is.null(input$search_text)) "" else input$search_text)
    if (nzchar(q)) {
      docs <- dplyr::filter(
        docs,
        grepl(tolower(q), tolower(.data$search_text), fixed = TRUE)
      )
    }

    docs
  })

  output$summary_text <- shiny::renderUI({
    docs <- filtered_docs()
    full_text_count <- sum(nzchar(documents$text_extracted), na.rm = TRUE)

    shiny::HTML(sprintf(
      paste0(
        "<strong>%s</strong> of <strong>%s</strong> documents shown. ",
        "<strong>%s</strong> documents currently include extracted text. ",
        "<strong>Last refreshed:</strong> %s."
      ),
      format(nrow(docs), big.mark = ","),
      format(nrow(documents), big.mark = ","),
      format(full_text_count, big.mark = ","),
      last_refreshed_text
    ))
  })

  output$documents_table <- DT::renderDT({
    docs <- filtered_docs() |>
      dplyr::transmute(
        `Meeting Date` = .data$meeting_date,
        `Fiscal Year` = .data$fiscal_year,
        `Document Type` = .data$doc_type,
        `Meeting Type` = .data$meeting_type,
        Title = .data$title,
        `Has Text` = ifelse(nzchar(.data$text_extracted), "Yes", "No"),
        Link = .data$link,
        Source = .data$source_system
      )

    DT::datatable(
      docs,
      escape = FALSE,
      rownames = FALSE,
      filter = "none",
      selection = "none",
      options = list(
        pageLength = 25,
        lengthMenu = c(10, 25, 50, 100),
        autoWidth = TRUE,
        dom = "tip"
      )
    )
  }, server = FALSE)
}

shiny::shinyApp(ui, server)
