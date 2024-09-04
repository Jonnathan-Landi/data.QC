#' Hard Limits Test for Meteorological Data
#'
#' This function applies a hard limits test to a specified meteorological variable in a `data.table`. Values are flagged based on whether they fall within specified upper and lower limits.
#'
#' @param dt A `data.table` containing the meteorological data. The data table must include the variable specified by `variable_analyzed`.
#' @param variable_analyzed A `character` string specifying the name of the meteorological variable to be analyzed (e.g., "TempAire_Avg", "RelativeHumidity").
#' @param LimInf A `numeric` value specifying the lower limit for the variable. Values below this limit are considered out of range.
#' @param LimSup A `numeric` value specifying the upper limit for the variable. Values above this limit are considered out of range.
#'
#' @return A `data.table` with an additional column `Flag_LimDuros` indicating the status of each value:
#' \itemize{
#'   \item `"C"` for values within the specified limits.
#'   \item `"M"` for values outside the specified limits.
#'   \item `"ND"` for values with missing data.
#' }
#' The function does not modify or remove the original variable column.
#'
#' @import data.table
#' @importFrom data.table fifelse
#'
#' @examples
#' # Create an example data.table
#' dt <- data.table(
#'   TIMESTAMP = seq.POSIXt(from = as.POSIXct("2024-01-01 00:00"),
#'                          to = as.POSIXct("2024-01-01 12:00"), by = "hour"),
#'   TempAire_Avg = c(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32))
#'
#' # Apply the hard limits test
#' result <- hard_limits(dt, "TempAire_Avg", 22, 28)
#' print(result)
#' @export
################################################################################
hard_limits = function(dt, variable_analyzed, LimInf, LimSup) {
  dt_hard_limits = dt[, Flag_LimDuros := fifelse(is.na(get(variable_analyzed)), "ND",
                                                 fifelse(get(variable_analyzed) >= LimInf &
                                                           get(variable_analyzed) <= LimSup, "C", "M"))]
  #dt_limtsDuros[, (variable_analyzed) := NULL]
  return(dt_hard_limits)
}
