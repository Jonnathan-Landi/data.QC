hard_limits = function(dt, variable_analyzed, LimInf, LimSup) {
  dt_hard_limits = dt[, Flag_LimDuros := fifelse(is.na(get(variable_analyzed)), "ND",
                                                 fifelse(get(variable_analyzed) >= LimInf &
                                                           get(variable_analyzed) <= LimSup, "C", "M"))]
  #dt_limtsDuros[, (variable_analyzed) := NULL]
  return(dt_hard_limits)
}
