options(warn = 1)

project_root <- "/Users/D/R Working Directory/schertz_council_docs_r2_app"
app_name <- "schertz-council-docs-phase-2"

message("=== Local refresh + deploy started ===")
message("Start time: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))

if (!dir.exists(project_root)) {
  stop("Project root does not exist: ", project_root)
}

setwd(project_root)

if (!file.exists("scripts/update_data.R")) {
  stop("Could not find scripts/update_data.R in project root.")
}

if (!file.exists("app.R")) {
  stop("Could not find app.R in project root.")
}

if (!file.exists("R/00_config.R")) {
  stop("Could not find R/00_config.R in project root.")
}

message("Project root: ", getwd())

Sys.setenv(
  SCHERTZ_INCLUDE_ARCHIVE = "false",
  SCHERTZ_START_YEAR = as.character(as.integer(format(Sys.Date(), "%Y")) - 1L)
)

# Step 1: refresh data
source("scripts/update_data.R")

# Step 2: verify required files exist before deploy
required_files <- c(
  "app.R",
  "R/00_config.R",
  "data/derived/documents.rds",
  "data/derived/refresh_metadata.csv"
)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Cannot deploy. Missing required files:\n",
    paste(missing_files, collapse = "\n")
  )
}

message("Refresh completed. Required deploy files are present.")

# Step 3: deploy only the files the app actually needs
rsconnect::deployApp(
  appDir = ".",
  appName = app_name,
  appFiles = c(
    "app.R",
    "R/00_config.R",
    "data/derived/documents.rds",
    "data/derived/refresh_metadata.csv"
  ),
  forceUpdate = TRUE
)

message("=== Local refresh + deploy finished successfully ===")
message("End time: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
