coherence_test <- function(dt, variable_analyzed, ventana) {
  # Ensure data.table operations are correctly applied
  dt_coherence <- data.table::copy(dt)

  # Column names for mean and standard deviation
  col_mean <- paste0(variable_analyzed, "_mean")
  col_sd <- paste0(variable_analyzed, "_desvst")

  # Calculate rolling mean and standard deviation
  dt_coherence[, (col_mean) := data.table::frollmean(get(variable_analyzed), n = ventana, align = "right")]
  dt_coherence[, (col_sd) := data.table::frollapply(get(variable_analyzed), n = ventana, sd, align = "right")]

  # Apply coherence test
  dt_coherence[, flag_coherence := {
    # Ensure proper referencing within the data.table environment
    value <- get(variable_analyzed)
    mean_value <- get(col_mean)
    sd_value <- get(col_sd)

    # Coherence check
    if (is.na(value)) {
      "ND"
    } else if (!is.na(mean_value) & !is.na(sd_value)) {
      if (value >= (mean_value - 3 * sd_value) & value <= (mean_value + 3 * sd_value)) {
        "C"
      } else {
        "D"
      }
    } else {
      "ND"
    }
  }]

  # Remove temporary columns
  dt_coherence[, c(col_mean, col_sd, variable_analyzed) := NULL]

  return(dt_coherence)
}
