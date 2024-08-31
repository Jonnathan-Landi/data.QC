persistence_test = function(dt, variable_analyzed, n_persistence) {
  dt_persistence = copy(dt)
  col_names = paste0(variable_analyzed, "_lag", 1:n_persistence)
  
  # Crear columnas de desfase
  for (i in 1:n_persistence) {
    dt_persistence[, (col_names[i]) := shift(get(variable_analyzed), n = i, type = "lag")]
  }
  
  # Determinar si la variable analizada es radiación solar
  radiation_variable = (variable_analyzed == "RS_W_Avg")
  
  all_equal = function(row) {
    if (any(is.na(row))) {
      return(FALSE)  # Retorna FALSE si hay algún NA en la fila
    }
    length(unique(row)) == 1  # Verificar si todos los valores no NA son iguales
  }
  
  if (radiation_variable == TRUE){
    dt_persistence[, all_equal := apply(.SD, 1, all_equal), .SDcols = setdiff(names(dt_persistence), "TIMESTAMP")]
    dt_persistence[, hour := hour(TIMESTAMP)]
    dt_persistence[, overnight := hour <= 5 | hour >= 18]
    dt_persistence[, hour := NULL]
    dt_persistence[, Persistence_flag := fifelse(is.na(get(variable_analyzed)), "ND",
                                                      fifelse(overnight, "ND",
                                                              fifelse(all_equal, "D", "C")))]
    
    inds = which(dt_persistence$Persistence_flag == "D")
    secuencias = unlist(lapply(inds, function(indice) {
      if (indice > n_persistence) {
        return((indice - n_persistence):(indice - 1))
      } else {
        return(integer(0))  # Retornar un vector vacío si el índice no permite una secuencia completa
      }
    }))
    
    if (length(inds) > 0) {
      dt_persistence[secuencias, Persistence_flag := "D"]
    }
    
    dt_persistence[, c((col_names), (variable_analyzed)) := NULL]
    dt_persistence[, overnight := NULL]
    dt_persistence[, all_equal := NULL]
    
  } else {
    # Aplicar la función a cada fila
    dt_persistence[, all_equal := apply(.SD, 1, all_equal), .SDcols = setdiff(names(dt_persistence), "TIMESTAMP")]
    dt_persistence[, Persistence_flag := fifelse(is.na(get(variable_analyzed)), "ND",
                                                      fifelse(all_equal, "D", "C"))]
    
    inds = which(dt_persistence$Persistence_flag == "D")
    if (length(inds) > 0) {
      secuencias = unlist(lapply(inds, function(indice) {
        if (indice > n_persistence) {
          return((indice - n_persistence):(indice - 1))
        } else {
          return(integer(0))  # Retornar un vector vacío si el índice no permite una secuencia completa
        }
      }))
      
      dt_persistence[secuencias, Persistence_flag := "D"]
    }
    dt_persistence[, c((col_names), (variable_analyzed)) := NULL]
    dt_persistence[, all_equal := NULL]
  } # cierro el else.
  return(dt_persistence)
}
