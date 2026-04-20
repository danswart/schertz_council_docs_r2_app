source("R/00_config.R")

schertz_r2_config <- function() {
  bucket <- Sys.getenv("CF_R2_BUCKET", unset = "")
  endpoint <- Sys.getenv("CF_R2_ENDPOINT", unset = "")
  key <- Sys.getenv("CF_R2_ACCESS_KEY_ID", unset = "")
  secret <- Sys.getenv("CF_R2_SECRET_ACCESS_KEY", unset = "")

  vals <- c(
    CF_R2_BUCKET = bucket,
    CF_R2_ENDPOINT = endpoint,
    CF_R2_ACCESS_KEY_ID = key,
    CF_R2_SECRET_ACCESS_KEY = secret
  )

  missing_names <- names(vals)[vals == ""]
  if (length(missing_names) > 0) {
    stop(
      "Missing R2 environment variables: ",
      paste(missing_names, collapse = ", "),
      call. = FALSE
    )
  }

  endpoint_host <- sub("^https?://", "", endpoint)
  endpoint_host <- sub("/$", "", endpoint_host)

  list(
    bucket = bucket,
    endpoint = endpoint,
    endpoint_host = endpoint_host,
    access_key = key,
    secret_key = secret
  )
}

schertz_r2_object_key <- function(filename) {
  paste0("raw/", basename(filename))
}

schertz_r2_apply_env <- function(cfg = schertz_r2_config()) {
  Sys.setenv(
    AWS_ACCESS_KEY_ID = cfg$access_key,
    AWS_SECRET_ACCESS_KEY = cfg$secret_key,
    AWS_DEFAULT_REGION = ""
  )

  invisible(cfg)
}

schertz_r2_put <- function(local_file, object_key = schertz_r2_object_key(local_file), cfg = schertz_config) {
  cfg_r2 <- schertz_r2_apply_env()

  if (!file.exists(local_file)) {
    stop("Local file does not exist: ", local_file, call. = FALSE)
  }

  file_size <- file.info(local_file)$size
  use_multipart <- !is.na(file_size) && file_size >= cfg$large_file_threshold_bytes

  invisible(
    aws.s3::put_object(
      file = local_file,
      object = object_key,
      bucket = cfg_r2$bucket,
      base_url = cfg_r2$endpoint_host,
      region = "",
      use_https = TRUE,
      multipart = use_multipart,
      show_progress = FALSE
    )
  )
}

schertz_r2_exists <- function(object_key) {
  cfg <- schertz_r2_apply_env()

  out <- suppressWarnings(
    suppressMessages(
      tryCatch(
        aws.s3::object_exists(
          object = object_key,
          bucket = cfg$bucket,
          base_url = cfg$endpoint_host,
          region = "",
          use_https = TRUE
        ),
        error = function(e) FALSE
      )
    )
  )

  isTRUE(out)
}

schertz_r2_download <- function(object_key, destfile) {
  cfg <- schertz_r2_apply_env()

  dir.create(dirname(destfile), recursive = TRUE, showWarnings = FALSE)

  ok <- aws.s3::save_object(
    object = object_key,
    bucket = cfg$bucket,
    file = destfile,
    base_url = cfg$endpoint_host,
    region = "",
    use_https = TRUE
  )

  if (!isTRUE(ok) && !file.exists(destfile)) {
    stop("Failed to download object from R2: ", object_key, call. = FALSE)
  }

  invisible(destfile)
}
