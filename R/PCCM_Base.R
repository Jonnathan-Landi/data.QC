################################################################################
# Autor: Jonnathan Landi
# Correo: jonnathana.landi@ucuenca.edu.ec / jonnathan.landi@outlook.com
# Versión: 3.0.1
################################################################################
#                            Funciones complementarias 
################################################################################
dircs = this.path()
dir_FunBase = dirname(dircs)
dir_FunBase = paste0(dir_FunBase, "/PCCM_FunBase.R")

env = new.env()
sys.source(dir_FunBase, envir = env)

leer.datos = function(directory, nombre.estat) {
  variables_soportadas = c(
    "TempAire_Min", "TempAire_Avg", "TempAire_Max",
    "HumAire_Min", "HumAire_Avg", "HumAire_Max",
    "BP_mbar_Min", "BP_mbar_Avg", "BP_mbar_Max",
    "RS_W_Avg", "VelocidadViento", "Lluvia_Tot", "DireccionViento"
  )
  estacion = paste0(nombre.estat, ".csv")
  file_path = file.path(directory, estacion)
  columnas_disponibles = names(fread(file_path, nrows = 0))
  columnas_a_cargar = intersect(variables_soportadas, columnas_disponibles)
  columnas_a_cargar = c("TIMESTAMP", columnas_a_cargar) 
  
  dt = fread(file_path, select = columnas_a_cargar)
  dt[, TIMESTAMP := as.POSIXct(TIMESTAMP, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")]
  dt[, (setdiff(names(dt), "TIMESTAMP")) := lapply(.SD, as.numeric), .SDcols = -1]
  setorder(dt, TIMESTAMP)
  message("Archivo cargado exitosamente.")
  return(dt)
}
################################################################################
#                        Variables soportadas actualmente
################################################################################
claves_variables = list(
  TempAire = c("TempAire_Min", "TempAire_Avg", "TempAire_Max"),
  HumAire = c("HumAire_Min", "HumAire_Avg", "HumAire_Max"),
  Vviento = c("VelocidadViento"),
  DirViento = c("DireccionViento"),
  RSolar = c("RS_W_Avg"),
  Lluvia = c("Lluvia_Tot"),
  BP_mbar = c("BP_mbar_Min", "BP_mbar_Avg", "BP_mbar_Max")
)
################################################################################
#     Control de Calidad de datos en intervalos de 5 min Nivel I (Automático)
################################################################################
minutes_control = function(dt) {
  
  resultados_analisis = list()
  
  obtener_clave_variable = function(variable_analizada) {
    for (nombre_clave in names(claves_variables)) {
      if (variable_analizada %in% claves_variables[[nombre_clave]]) {
        return(nombre_clave)
      }
    }
    stop("La variable no se encuentra en el archivo de claves.")
  }
  
  secuencia_ideal = data.table(TIMESTAMP = seq.POSIXt(from = min(dt$TIMESTAMP), 
                                                      to = max(dt$TIMESTAMP), by = "5 min"))
  # ----------------------------------------------------------------------------
  dt_5minfinal = secuencia_ideal
  
  for (variable in setdiff(names(dt), "TIMESTAMP")) {
    message("Test de duplicados exactos en ejecución....")
    print(variable)
    dt_5min = dt[, .(TIMESTAMP, get(variable))]
    setnames(dt_5min, c("TIMESTAMP", variable))
    
    dt_5min[, temp_var := ifelse(is.na(get(variable)) & duplicated(TIMESTAMP), "NA_PLACEHOLDER", get(variable))]
    # Elimino todas las filas que tengan NA_PLACEHOLDER
    dt_5min = dt_5min[temp_var != "NA_PLACEHOLDER"]
    dt_5min[, temp_var := NULL]
    
    # Identificación y eliminación de fechas duplicadas exactas ------------------
    dt_5min[, duplicated_flag := duplicated(.SD, by = c("TIMESTAMP", variable)) | 
              duplicated(.SD, by = c("TIMESTAMP", variable), fromLast = TRUE)]
    
    fechas_duplicadas = dt_5min[duplicated_flag == TRUE] # Pal reporte
    
    if (any(dt_5min$duplicated_flag)) {
      datos.duplicados = dt_5min[duplicated_flag == TRUE][order(TIMESTAMP)]
      dt_5min = unique(dt_5min, by = c("TIMESTAMP", variable))
      dt_5min[, duplicated_flag := NULL]
      
    } else {
      dt_5min[, duplicated_flag := NULL]
      datos.duplicados = NULL
    }
    
    dt_5min = merge(secuencia_ideal, dt_5min, by = "TIMESTAMP", all = TRUE)
    dt_5min = dt_5min[order(TIMESTAMP)]
    
    # Secuencia de datos incorrecta (Datos en intervalos != 5 minutos) ---------
    
    # Identificación de fechas iguales pero valores diferentes -----------------
    dt_5min[,duplicated_flag := duplicated(.SD, by = "TIMESTAMP") | 
              duplicated(.SD, by = "TIMESTAMP", fromLast = TRUE)]
    if (any(dt_5min$duplicated_flag == TRUE)) {
      dt_5min[, duplicated_flag := NULL]
      dt_5min = env$duplicados_diferentes(dt_5min, variable, 3)
    } else {
      dt_5min[, duplicated_flag := NULL]
    }
    
    dt_5minfinal = merge(dt_5minfinal, dt_5min, by = "TIMESTAMP", all = TRUE)
    
    if (nrow(dt_5minfinal) != nrow(secuencia_ideal)) {
      stop("Error en la longitud de los datos")
    }
  }
  
  # Limites duros --------------------------------------------------------------
  limites = list(
    TempAire = c(-20, 45), #grados centigrados
    HumAire = c(0,100),
    Vviento = c(0, 75), #m/s
    DirViento = c(0, 360), #grados
    RSolar = c(0, 1400), #W/m2
    Lluvia = c(0, 40), #mm
    BP_mbar = c(300, 1100)
  )
  
  dt_5_2 = secuencia_ideal
  for (variable in setdiff(names(dt_5minfinal), "TIMESTAMP")) {
    message("Test de limites duros en ejecución....")
    print(variable)
    # Determinar la clave y límites para la variable actual
    clave = obtener_clave_variable(variable)
    LimInf = limites[[clave]][1]
    LimSup = limites[[clave]][2]
    
    # Subconjunto de datos y renombrado directo usando data.table
    dt_temp = dt_5minfinal[, .(TIMESTAMP, get(variable))]
    setnames(dt_temp, c("TIMESTAMP", variable))
    
    # Función de límites duros. 
    resultados_analisis$Test_limitesDuros[[variable]] = env$limites_duros(dt_temp, variable, LimInf, LimSup)
    inds_inf = which(dt_temp[[variable]] < LimInf)
    inds_sup = which(dt_temp[[variable]] > LimSup)
    
    dt_temp = dt_temp[, (variable) := fifelse(
      is.na(get(variable)) | get(variable) < LimInf | get(variable) > LimSup,
      NA_real_,
      get(variable)
    )]
    
    dt_5_2 = merge(dt_5_2, dt_temp, by = "TIMESTAMP", all = TRUE)
    
    resultados_analisis$LimiteInferior[[variable]] = dt_temp[inds_inf]
    resultados_analisis$LimiteSuperior[[variable]] = dt_temp[inds_sup]
    
  }
  
  if (nrow(dt_5_2) != nrow(secuencia_ideal)) {
    stop("Error en la longitud de los datos")
  }
  
  # Posibles fallas sensor -----------------------------------------------------
  dt_ideal = secuencia_ideal
  umbrales_rangos = list(
    TempAire = list(umbral = c(4), permitido = NULL),
    HumAire = list(umbral = c(4), permitido = c(100)),
    Vviento = list(umbral = c(4), permitido = c(0)),
    DirViento = list(umbral = c(4), permitido = c(0)),
    BP_mbar = list(umbral = c(4), permitido = NULL),
    Lluvia = list(umbral = c(4), permitido = c(0)),
    RSolar = list(umbral = c(4), permitido = c(0))
  )
  
  for (variable in setdiff(names(dt_5_2), "TIMESTAMP")) {
    dt_temp = dt_5_2[, .(TIMESTAMP, get(variable))]
    setnames(dt_temp, c("TIMESTAMP", variable))
    clave = obtener_clave_variable(variable)
    
    if (clave %in% names(umbrales_rangos)) {
      message("Test de posibles fallas en el sensor....")
      print(variable)
      umbrales = umbrales_rangos[[clave]]$umbral
      permitidos = umbrales_rangos[[clave]]$permitido
      
      Nas_actuales = sum(is.na(dt_temp[[variable]]))
      
      rle_result = rle(dt_temp[[variable]])
      dt_temp$rep = rep(rle_result$lengths, rle_result$lengths)
      ind.umbral = which(dt_temp$rep >= umbrales)
      
      fallos.sensor = dt_temp[ind.umbral,]
      values_permitidos = NULL
      total_remplazados_NA = NULL
      
      if (length(ind.umbral) > 0){
        fallos.sensor = dt_temp[ind.umbral,]
        # Descartar eventos permitidos 
        if (!is.null(permitidos)) {
          ind.permitidos = which(dt_temp$rep >= umbrales & dt_temp[[variable]] != permitidos)
          values_permitidos = which(dt_temp$rep >= umbrales & dt_temp[[variable]] == permitidos)
          fallos.sensor = dt_temp[ind.permitidos,]
          dt_temp[ind.permitidos, (variable) := NA]
        } else {
          fallos.sensor = dt_temp[ind.umbral,]
          dt_temp[ind.umbral, (variable) := NA]
        }
        
        total_remplazados_NA = sum(is.na(dt_temp[[variable]])) - Nas_actuales
      }
      
      dt_temp[, rep := NULL]
      fallos.sensor = fallos.sensor[, rep := NULL]
      resultados_analisis$Posibles_fallas[[variable]] = fallos.sensor
    }
    
    if (nrow(dt_temp) !=  nrow(dt_ideal)) {
      stop("Error en la longitud de los datos")
    } else {
      dt_ideal = merge(dt_ideal, dt_temp, by = "TIMESTAMP", all = TRUE)
    }
  }
  
  dt_final = dt_ideal
  for (variable in setdiff(names(dt_final), "TIMESTAMP")) {
    Reporte = data.frame(
      variable_analizada = variable,
      fecha_Registro_Inicial = min(dt_final$TIMESTAMP),
      fecha_Registro_Final = max(dt_final$TIMESTAMP),
      total_NA = sum(is.na(dt_final[[variable]])),
      Porcentaje_vacios = sum(is.na(dt_final[[variable]])) / nrow(dt_final) * 100
    )
    resultados_analisis$Reporte[[variable]] = Reporte
  }
  # Resultado final ------------------------------------------------------------
  test_types = list(
    Test_fechasDuplicadas = resultados_analisis$fechas_duplicadss,
    Test_limitesDuros = resultados_analisis$Test_limitesDuros,
    Test_LimiteInferior = resultados_analisis$LimiteInferior,
    Test_LimiteSuperior = resultados_analisis$LimiteSuperior,
    Test_PosiblesFallasSensor = resultados_analisis$Posibles_fallas,
    Reporte = resultados_analisis$Reporte
  )
  
  variables = unique(unlist(lapply(test_types, names)))
  
  nueva_lista = lapply(variables, function(variable) {
    # Para cada variable, recolectar los resultados de todos los tipos de test
    resultados = lapply(test_types, function(test_list) {
      if (variable %in% names(test_list)) {
        return(test_list[[variable]])
      }
      return(NULL)
    })
    
    # Crear una lista con nombres de los tests y sus resultados
    names(resultados) = names(test_types)
    
    # Filtrar los resultados no nulos
    resultados = Filter(Negate(is.null), resultados)
    return(resultados)
  })
  # Asignar nombres a la nueva lista
  names(nueva_lista) = variables
  resultados_analisis = NULL
  Reporte_5min <<- nueva_lista
  return(dt_final)
}

################################################################################
#          Control de Calidad de datos horarios de Nivel I (Automático)
################################################################################
hourly_pooling = function(dt){
  inicio_hora = floor_date(min(dt$TIMESTAMP), unit = "hour")
  fin_hora = floor_date(max(dt$TIMESTAMP), unit = "hour")
  
  # Crear secuencia desde el tiempo redondeado hasta el máximo tiempo
  secuencia_ideal = data.table(TIMESTAMP = seq.POSIXt(from = inicio_hora, 
                                                      to = fin_hora, 
                                                      by = "hour"))
  # Uno con mi set de datos 
  dt_hourly = secuencia_ideal
  
  fun_agrup = list(
    TempAire_Min = "min",
    TempAire_Avg = "mean",
    TempAire_Max = "max",
    HumAire_Min = "min",
    HumAire_Avg = "mean",
    HumAire_Max = "max",
    BP_mbar_Min = "min",
    BP_mbar_Avg = "mean",
    BP_mbar_Max = "max",
    RS_W_Avg = "mean",
    DireccionViento = "mean",
    VelocidadViento = "mean",
    Lluvia_Tot = "sum"
  )
  
  for (variable in setdiff(names(dt), "TIMESTAMP")) {
    dt_temp = dt[, .(TIMESTAMP, get(variable))]
    setnames(dt_temp, c("TIMESTAMP", variable))
    if (variable %in% names(fun_agrup)) {
      fun_apli = fun_agrup[[variable]]
      message("agrupando datos de:....", "(",fun_apli,")")
      print(variable)
      fun_apli = match.fun(fun_apli)
      
      dt_temp = dt_temp[, .(
        variable = if (sum(!is.na(get(variable))) >= 11) {
          fun_apli(get(variable), na.rm = TRUE)
        } else {
          NA_real_
        }
      ), by = .(TIMESTAMP = floor_date(TIMESTAMP, "hour"))]
      
      setnames(dt_temp, c("TIMESTAMP", variable))
      
      dt_temp[is.infinite(get(variable)), (variable) := NA_real_]
      dt_temp[is.nan(get(variable)), (variable) := NA_real_]
      
      dt_hourly = merge(dt_hourly, dt_temp, by = "TIMESTAMP", all = TRUE)
      
    }
  }
  
  c1 = nrow(secuencia_ideal)
  print(c1)
  c2 = nrow(dt_hourly)
  print(c2)
  if (c1 != c2) {
    stop("Código de error 004")
  }
  return(dt_hourly)
}

hourly_control = function(dt) {
  resultados_analisis = list()
  # Test de limites duros ------------------------------------------------------
  limites = list(
    TempAire = c(-20, 45), #grados centigrados
    HumAire = c(0,100),
    Vviento = c(0, 75), #m/s
    DirViento = c(0, 360), #grados
    RSolar = c(-1, 1400), #W/m2
    Lluvia = c(0, 401), #mm
    BP_mbar = c(300, 1100)
  )
  
  obtener_clave_variable = function(variable_analizada) {
    for (nombre_clave in names(claves_variables)) {
      if (variable_analizada %in% claves_variables[[nombre_clave]]) {
        return(nombre_clave)
      }
    }
    stop("La variable no se encuentra en el archivo de claves.")
  }
  
  # Test de limites duros ------------------------------------------------------
  for (variable in setdiff(names(dt), "TIMESTAMP")) {
    message("Test de limites duros en ejecución....")
    print(variable)
    # Determinar la clave y límites para la variable actual
    clave = obtener_clave_variable(variable)
    LimInf = limites[[clave]][1]
    LimSup = limites[[clave]][2]
    
    # Subconjunto de datos y renombrado directo usando data.table
    dt_temp = dt[, .(TIMESTAMP, get(variable))]
    setnames(dt_temp, c("TIMESTAMP", variable))
    
    # Función de límites duros. 
    resultados_analisis$Test_limitesDuros[[variable]] = env$limites_duros(dt_temp, variable, LimInf, LimSup)
  }
  
  # Test de consistencia y velocidad viento ------------------------------------
  dt_DV_viento = dt[, .(TIMESTAMP, VelocidadViento, DireccionViento)]
  setnames(dt_DV_viento, c("TIMESTAMP", "VelocidadViento", "DireccionViento"))
  dt_test = env$Test_VD_viento(dt_DV_viento)
  resultados_analisis$Test_VD_viento[["VelocidadViento"]] = dt_test
  resultados_analisis$Test_VD_viento[["DireccionViento"]] = dt_test
  # Test_paso ------------------------------------------------------------------
  lags_umbrals = list(
    TempAire = list(lags = c(1, 2, 3, 6, 12), umbrales = c(4, 7, 9, 15, 25)),
    HumAire = list(lags = c(1), umbrales = c(45)),
    Vviento = list(lags = c(1), umbrales = c(10)),
    BP_mbar = list(lags = c(1, 2, 3, 6, 12), umbrales = c(3, 6, 9, 18, 36)),
    RSolar = list(lags = c(1), umbrales = c(555))
  )
  
  for (variable in setdiff(names(dt), "TIMESTAMP")) {
    # Determinar la clave y límites para la variable actual
    clave = obtener_clave_variable(variable)
    
    if (clave %in% names(lags_umbrals)) {
      message("Test de paso en ejecución....")
      print(variable)
      lags = lags_umbrals[[clave]]$lags
      umbrales = lags_umbrals[[clave]]$umbrales
      
      # Subconjunto de datos y renombrado directo usando data.table
      dt_temp = dt[, .(TIMESTAMP, get(variable))]
      setnames(dt_temp, c("TIMESTAMP", variable))
      # Función de límites duros. 
      resultados_analisis$Test_paso[[variable]] = env$Test_paso(dt_temp, variable, lags, umbrales)
    }
  }
  
  # paso dirección del viento
  dt_temp = dt[, .(TIMESTAMP, DireccionViento)]
  setnames(dt_temp, c("TIMESTAMP", "DireccionViento"))
  resultados_analisis$Test_paso[["DireccionViento"]] = env$paso_DV(dt_temp, "DireccionViento")
  # Test de persistencia -------------------------------------------------------
  persistencias = list(
    TempAire = 3,
    HumAire = 3,
    Vviento = 3,
    BP_mbar = 11,
    RSolar = 3,
    DirViento = 3
  )
  
  for (variable in setdiff(names(dt), "TIMESTAMP")) {
    # Determinar la clave y límites para la variable actual
    clave = obtener_clave_variable(variable)
    
    if (clave %in% names(persistencias)) {
      message("Test de persistencia en ejecución....")
      print(variable)
      n_persistencias = persistencias[[clave]]
      
      # Subconjunto de datos y renombrado directo usando data.table
      dt_temp = dt[, .(TIMESTAMP, get(variable))]
      setnames(dt_temp, c("TIMESTAMP", variable))
      # Función de límites duros. 
      resultados_analisis$Test_persistencia[[variable]] = env$Test_persistencia(dt_temp, variable, n_persistencias)
    }
  }
  # Test de coherencia ---------------------------------------------------------
  ventanas_coherencia = list(
    TempAire = 5,
    HumAire = 5
  )
  for (variable in setdiff(names(dt), "TIMESTAMP")) {
    # Determinar la clave y límites para la variable actual
    clave = obtener_clave_variable(variable)
    
    if (clave %in% names(ventanas_coherencia)) {
      message("Test de coherencia en ejecución....")
      print(variable)
      ventana = ventanas_coherencia[[clave]]
      
      # Subconjunto de datos y renombrado directo usando data.table
      dt_temp = dt[, .(TIMESTAMP, get(variable))]
      setnames(dt_temp, c("TIMESTAMP", variable))
      # Función de límites duros. 
      resultados_analisis$Test_coherencia[[variable]] = env$Test_coherencia(dt_temp, variable, ventana)
    }
  }
  # Resultado final ------------------------------------------------------------
  for (variable in setdiff(names(dt), "TIMESTAMP")) {
    Reporte = data.frame(
      variable_analizada = variable,
      fecha_Registro_Inicial = min(dt$TIMESTAMP),
      fecha_Registro_Final = max(dt$TIMESTAMP),
      total_NA = sum(is.na(dt[[variable]])),
      Porcentaje_vacios = sum(is.na(dt[[variable]])) / nrow(dt) * 100
    )
  #  resultados_analisis$Reporte[[variable]] = Reporte
  }
  
  test_types = list(
    Test_limitesDuros = resultados_analisis$Test_limitesDuros,
    Test_paso = resultados_analisis$Test_paso,
    Test_persistencia = resultados_analisis$Test_persistencia,
    Test_coherencia = resultados_analisis$Test_coherencia,
    Test_VD_viento = resultados_analisis$Test_VD_viento
 #   Reporte_general = resultados_analisis$Reporte
  )
  
  variables = unique(unlist(lapply(test_types, names)))
  
  nueva_lista = lapply(variables, function(variable) {
    # Para cada variable, recolectar los resultados de todos los tipos de test
    resultados = lapply(test_types, function(test_list) {
      if (variable %in% names(test_list)) {
        return(test_list[[variable]])
      }
      return(NULL)
    })
    
    # Crear una lista con nombres de los tests y sus resultados
    names(resultados) = names(test_types)
    
    # Filtrar los resultados no nulos
    resultados = Filter(Negate(is.null), resultados)
    return(resultados)
  })
  # Asignar nombres a la nueva lista
  names(nueva_lista) = variables
  resultados_analisis = NULL
  
  merge_tests = function(tests_list) {
    merged_data = Reduce(function(x, y) merge(x, y, by = "TIMESTAMP", all = TRUE), tests_list)
    return(merged_data)
  }
  
  # Aplicar la función de merge para cada grupo de variables
  result_list = lapply(nueva_lista, merge_tests)
  
  # informe final
  result_list = lapply(result_list, function(dt) {
    n = ncol(dt[, -1])
    dt[, Result := fcase(
      rowSums(dt[, -1] == "D", na.rm = TRUE) >= n, "Erroneo",  # Condición para cuando todas las banderas son "D"
      rowSums(dt[, -1] == "ND", na.rm = TRUE) >= n, "Aceptable",  # Condición para cuando todas las banderas son "ND"
      rowSums(dt[, -1] == "D", na.rm = TRUE) > rowSums(dt[, -1] == "C", na.rm = TRUE), "Erroneo",  # Más "D" que "C"
      rowSums(dt[, -1] == "D", na.rm = TRUE) == rowSums(dt[, -1] == "C", na.rm = TRUE), "Dudoso",  # Igual número de "D" y "C"
      rowSums(dt[, -1] == "D", na.rm = TRUE) < rowSums(dt[, -1] == "C", na.rm = TRUE), "Aceptable",  # Más "C" que "D"
      default = "Aceptable"  # Valor por defecto
    )]
    return(dt)
  })
  
  
  List_exportar = list()
  for (variable in setdiff(names(dt), "TIMESTAMP")) {
    merged_data  = merge(result_list[[variable]], dt[, .(TIMESTAMP, get(variable))], by = "TIMESTAMP", all = TRUE)
    setnames(merged_data, old = "V2", new = variable)
    List_exportar[[variable]] <- merged_data
  }
  
  # Crear un libro de Excel con los resultados
  # Creo una carpeta para guardar los resultados
  carpeta = paste0(directory, "/Reporte_", nombre.estat)
  if (!dir.exists(carpeta)) {
    dir.create(carpeta)
  }
  wb = createWorkbook()
  for (i in seq_along(List_exportar)) {
    sheet_name <- names(List_exportar)[i]
    addWorksheet(wb, sheetName = sheet_name)
    writeData(wb, sheet = sheet_name, List_exportar[[i]])
  }
  message("Guardando el reporte en Excel...")
  saveWorkbook(wb, file = paste0(carpeta,"/Reporte_variables.xlsx"), overwrite = TRUE)
  # ----------------------------------------------------------------------------
  return(List_exportar)
}

corregir_datos = function(list, dt) {
  reporte = list()
  for (variable in names(list)) {
    print(variable)
    fechas_erroneas <- list[[variable]][Result == "Erroneo", TIMESTAMP]
    
    if (variable %in% names(dt)) {
      dt[get("TIMESTAMP") %in% fechas_erroneas, (variable) := NA]
    }
  }
  
  for (variable in setdiff(names(dt), "TIMESTAMP")) {
    
    Reporte = data.frame(
      variable_analizada = variable,
      fecha_Registro_Inicial = min(dt$TIMESTAMP),
      fecha_Registro_Final = max(dt$TIMESTAMP),
      total_NA = sum(is.na(dt[[variable]])),
      Porcentaje_vacios = sum(is.na(dt[[variable]])) / nrow(dt) * 100
    )
    reporte[[variable]] = Reporte
  }
  names = paste0(directory, "/datos_HH_", nombre.estat, ".csv")
  write.csv(dt, file = names, row.names = FALSE)
  reporte_horario <<- reporte
  return(dt)
}


