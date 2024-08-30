step_test = function(dt, variable_analyzed, lags, thresholds) {
  dt_step = copy(dt)
  col_names = paste0("diff_h-", lags)
  for (lag in lags) {
    dt_step[, paste0("diff_h-", lag) := {
      diff_value <- abs(get(variable_analyzed) - shift(get(variable_analyzed), n = lag, type = "lag"))
      ifelse(is.na(get(variable_analyzed)), "ND", diff_value)
    }]
  }

  bandera_paso = paste0("Paso_", lags)

  for (i in seq_along(lags)) {
    dt_step[, (bandera_paso[i]) := {
      # Asignar banderas basadas en la diferencia y el umbral
      diff_col <- get(col_names[i])
      ifelse(is.na(diff_col), "ND", ifelse(diff_col <= thresholds[i], "C", "D"))
    }]
  }
  # elimino columnas temporaltes
#  dt_step[, c((col_names), (variable_analizada)) := NULL]
  dt_step[, (col_names) := NULL]
  return(dt_step)
}
