#' Coherence Test for Hourly Meteorological Data
#'
#' This function performs a coherence test on hourly meteorological data by calculating rolling mean and standard deviation for a specified variable. The test flags values based on their deviation from the rolling mean and standard deviation.
#'
#' @param dt A `data.table` containing the hourly meteorological data. The data table must include a `TIMESTAMP` column and the variable specified by `variable_analyzed`.
#' @param variable_analyzed A `character` string specifying the name of the meteorological variable to be analyzed (e.g., "TempAire_Avg", "RelativeHumidity").
#' @param ventana An `integer` specifying the number of preceding hourly records to consider for calculating the rolling mean (\eqn{\mu_1}) and standard deviation (\eqn{\sigma_1}).
#'
#' @return A `data.table` with an additional column `flag_coherence` indicating the coherence status of each value:
#' \itemize{
#'   \item `"C"` for coherent values (within 3 standard deviations of the rolling mean).
#'   \item `"D"` for discordant values (outside 3 standard deviations of the rolling mean).
#'   \item `"ND"` for values with insufficient data for coherence testing.
#' }
#' The function also removes the intermediate columns used for calculations.
#'
#' @import data.table
#'
#' @examples
#' # Create an example data.table
#' dt <- data.table(
#'   TIMESTAMP = seq.POSIXt(from = as.POSIXct("2024-01-01 00:00"),
#'                          to = as.POSIXct("2024-01-01 12:00"), by = "hour"),
#'   TempAire_Avg = c(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32))
#'
#' # Apply the coherence test
#' result <- coherence_test(dt, "TempAire_Avg", 5)
#' print(result)
#' @export
################################################################################
coherence_test = function(dt, variable_analyzed, ventana) {
  dt_coherence = copy(dt)
  col_mean = paste0(variable_analyzed, "_mean")
  col_sd = paste0(variable_analyzed, "_desvst")

  dt_coherence[, (col_mean) := frollmean(get(variable_analyzed), n = ventana, align = "right")]
  dt_coherence[, (col_sd) := frollapply(get(variable_analyzed), n = ventana, sd, align = "right")]

  dt_coherence[, flag_coherence := {
    ifelse(is.na(get(variable_analyzed)), "ND",
           ifelse(!is.na(get(col_mean)) & !is.na(get(col_sd)),
                  ifelse(get(variable_analyzed) >= (get(col_mean) - 3 * get(col_sd)) &
                           get(variable_analyzed) <= (get(col_mean) + 3 * get(col_sd)),
                         "C", "D"),
                  "ND"))
  }]
  dt_coherence[, c((col_mean), (col_sd), (variable_analyzed)) := NULL]
  return(dt_coherence)
}

