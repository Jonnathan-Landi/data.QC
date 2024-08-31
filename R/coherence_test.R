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
