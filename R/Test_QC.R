################################################################################
# Autor: Jonnathan Landi
# Correo: jonnathana.landi@ucuenca.edu.ec / jonnathan.landi@outlook.com
# Versión: 1.8.6 (Beta)


################################################################################
#             Control de Calidad de Ldatos de Nivel I (Automático)
################################################################################
# Funciones para los datos a escala 5 minutos ----------------------------------
secuencia_incorrecta = function(dt, variable_analizada){
  dt_duplicados = copy(dt)
  procesar_datos_paralelo = function(dt_5min, variable, num_cores) {
    plan(multicore, workers = num_cores)

    # Paso 1: Identificar intervalos incorrectos (vectorizado)
    dt_5min[, diff := c(NA, as.numeric(diff(TIMESTAMP, units = "mins")))]
    intervalos_incorrectos = dt_5min[diff != 5 & !is.na(diff) & diff != 0
                                     & is.na(get(variable))]

    if (nrow(intervalos_incorrectos) > 0) {
      message("ADVERTENCIA: Corrigiendo intervalos incorrectos en paralelo.")

      # Paso 2: Generar el objetivo para cada TIMESTAMP incorrecto (vectorizado)
      intervalos_incorrectos[, value_redondeado := ceiling_date(TIMESTAMP, "5 mins")]

      # Paso 3 y 4: Rellenar valores NA con el valor más cercano en paralelo
      procesar_chunk = function(chunk) {
        chunk[, valor_cercano := {
          window_start = TIMESTAMP - minutes(1)
          window_end = TIMESTAMP
          valores_cercanos = dt_5min[TIMESTAMP >= window_start & TIMESTAMP <= window_end, get(variable)]
          if (length(valores_cercanos) > 0) valores_cercanos[which.min(abs(difftime(dt_5min$TIMESTAMP[TIMESTAMP >= window_start & TIMESTAMP <= window_end], TIMESTAMP, units = "secs")))]
          else NA_real_
        }, by = TIMESTAMP]
        return(chunk)
      }

      # Dividir los datos en chunks para procesamiento paralelo
      chunks = split(intervalos_incorrectos, (seq(nrow(intervalos_incorrectos))-1) %/% (nrow(intervalos_incorrectos)/num_cores))

      # Procesar chunks en paralelo
      resultados = future_lapply(chunks, procesar_chunk)

      # Combinar resultados
      intervalos_corregidos = rbindlist(resultados)

      # Actualizar dt_5min con los valores corregidos
      dt_5min[intervalos_corregidos, (variable) := fifelse(is.na(get(variable)), i.valor_cercano, get(variable)), on = "TIMESTAMP"]

      # Paso 5: Verificar que los intervalos sean correctos (vectorizado)
      dt_5min <- dt_5min[second(TIMESTAMP) == 0 & minute(TIMESTAMP) %% 5 == 0]

      # Verificar la consistencia de los intervalos (vectorizado)
      dt_5min[, diff := c(NA, as.numeric(diff(TIMESTAMP, units = "mins")))]
      if (any(second(dt_5min$TIMESTAMP) != 0 & minute(dt_5min$TIMESTAMP) %% 5 != 0)) {
        stop("Código de error 001")
      }
    }
    return(dt_5min)
  }

  if (nrow(intervalos_incorrectos) > 0) {
    cl = detectCores() - 1
    dt_5min = procesar_datos_paralelo(dt_5min, variable, num_cores = cl)
  } else {

    dt_5min = dt_5min[second(TIMESTAMP) == 0 & minute(TIMESTAMP) %% 5 == 0]
    if (any(second(dt_5min$TIMESTAMP) != 0 & minute(dt_5min$TIMESTAMP) %% 5 != 0)) {
      stop("Código de error 001")
    }
  }

  inds_cero = which(dt_5min$diff == 0)
}

duplicados_diferentes = function(dt, variable_analizada, ventana, num_cores = parallel::detectCores() - 1) {
  message("Procedimiento de fechas iguales con diferentes valores en ejecución..........")
  print(paste0(variable_analizada, " con valores diferentes en fechas iguales"))
  # Configurar el plan de paralelización
  plan(multisession, workers = num_cores)

  # Crear una copia de la tabla para trabajar sin alterar el original
  dt_copia <- copy(dt)
  setkey(dt_copia, TIMESTAMP)

  # Función para calcular estadísticas
  calcular_estadisticas <- function(x) {
    media <- mean(x, na.rm = TRUE)
    mediana <- median(x, na.rm = TRUE)
    desv <- sd(x, na.rm = TRUE)
    list(media = media, mediana = mediana, Li = media - 2 * desv, Ls = media + 2 * desv, desv = desv)
  }

  # Función para contar éxitos
  contar_exitos <- function(valor, stats) {
    sum(
      between(valor, stats$Li, stats$Ls),
      between(valor, stats$media - 2 * stats$desv, stats$media + 2 * stats$desv),
      between(valor, stats$mediana - 2 * stats$desv, stats$mediana + 2 * stats$desv)
    )
  }

  # Función para procesar un timestamp duplicado
  procesar_timestamp <- function(ts, dt, variable_analizada, ventana) {
    ventana_inicio <- ts - hours(ventana)

    # Calcular estadísticas de la ventana anterior
    stats <- dt[TIMESTAMP >= ventana_inicio & TIMESTAMP < ts,
                calcular_estadisticas(.SD[[variable_analizada]])]

    # Evaluar valores duplicados
    resultados <- dt[TIMESTAMP == ts, .(
      value = .SD[[variable_analizada]],
      n_exitos = contar_exitos(.SD[[variable_analizada]], stats)
    )]

    # Determinar el valor real
    if (length(unique(resultados$n_exitos)) == 1) {
      valor_real <- mean(resultados$value[resultados$n_exitos == max(resultados$n_exitos)], na.rm = TRUE)
    } else {
      valor_real <- resultados[which.max(n_exitos), value]
    }

    list(ts = ts, valor_real = valor_real)
  }

  # Procesar duplicados
  print("Iniciando limpieza de datos")
  dup_totales = length(dt_copia[, duplicated(TIMESTAMP)])
  while (dt_copia[, any(duplicated(TIMESTAMP))]) {
    # Identificar timestamps duplicados
    dup_timestamps <- dt_copia[, .N, by = TIMESTAMP][N > 1, TIMESTAMP]

    # Procesar cada timestamp duplicado en paralelo
    resultados <- future_lapply(dup_timestamps, function(ts) {
      procesar_timestamp(ts, dt_copia, variable_analizada, ventana)
    }, future.seed = TRUE)

    # Actualizar valores en la tabla
    for (res in resultados) {
      dt_copia[TIMESTAMP == res$ts, (variable_analizada) := res$valor_real]
    }

    # Eliminar duplicados
    dt_copia <- unique(dt_copia, by = "TIMESTAMP")
    dups_restantes <- length(dt_copia[, duplicated(TIMESTAMP)])

  }

  # Cerrar el plan de paralelización
  plan(sequential)

  return(dt_copia)
}


# # DEBUG
# dt = copy(datos)
# variable_analizada = "BP_mbar_Avg"
# dt = dt[, .(TIMESTAMP, get(variable_analizada))]
# setnames(dt, c("TIMESTAMP", variable_analizada))

# ------------------------------------------------------------------------------
######################## Funciones de QC para datos horarios ###################
# 1. Test de limites duros -----------------------------------------------------
limites_duros = function(dt, variable_analizada, LimInf, LimSup) {
  dt_limtsDuros = copy(dt)
  dt_limtsDuros[, Flag_LimDuros := fifelse(is.na(get(variable_analizada)), "ND",
                                fifelse(get(variable_analizada) >= LimInf &
                                          get(variable_analizada) <= LimSup, "C", "M"))]
  dt_limtsDuros[, (variable_analizada) := NULL]
  return(dt_limtsDuros)
}

# 2. Test de consistencia temporal ---------------------------------------------
## 2.1 Test de paso.
Test_paso = function(dt, variable_analizada, lags, umbrales) {
  dt_paso = copy(dt)
  col_names = paste0("diff_h-", lags)
  for (lag in lags) {
    dt_paso[, paste0("diff_h-", lag) := {
      diff_value <- abs(get(variable_analizada) - shift(get(variable_analizada), n = lag, type = "lag"))
      ifelse(is.na(get(variable_analizada)), "ND", diff_value)
    }]
  }

  bandera_paso = paste0("Paso_", lags)

  for (i in seq_along(lags)) {
    dt_paso[, (bandera_paso[i]) := {
      # Asignar banderas basadas en la diferencia y el umbral
      diff_col <- get(col_names[i])
      ifelse(is.na(diff_col), "ND", ifelse(diff_col <= umbrales[i], "C", "D"))
    }]
  }
  # elimino columnas temporaltes
  dt_paso[, c((col_names), (variable_analizada)) := NULL]
  return(dt_paso)
}

paso_DV = function(dt, variable_analizada){
  valor = function(x) {
    diferencias = abs(x - shift(x, n = 1, type = "lag"))
    minimos = pmin(diferencias, 360 - diferencias, na.rm = TRUE)
    return(minimos)
  }
  dt_paso = dt[, Bandera_paso_dv := {
    fifelse(is.na(get(variable_analizada)), "ND",
            fifelse(valor(get(variable_analizada)) <= 150, "C", "D"))
  }]
  dt_paso = dt_paso[, (variable_analizada) := NULL]
  return(dt_paso)
}

## 2.2 Test de persistencia.
Test_persistencia = function(dt, variable_analizada, n_persistencias) {
  dt_persistencia <- copy(dt)
  col_names <- paste0(variable_analizada, "_lag", 1:n_persistencias)

  # Crear columnas de desfase
  for (i in 1:n_persistencias) {
    dt_persistencia[, (col_names[i]) := shift(get(variable_analizada), n = i, type = "lag")]
  }

  # Determinar si la variable analizada es radiación solar
  variable_es_radiacion <- variable_analizada == "RS_W_Avg"

  todos_iguales = function(row) {
    if (any(is.na(row))) {
      return(FALSE)  # Retorna FALSE si hay algún NA en la fila
    }
    length(unique(row)) == 1  # Verificar si todos los valores no NA son iguales
  }

  if (variable_es_radiacion == TRUE){
    dt_persistencia[, Todos_Iguales := apply(.SD, 1, todos_iguales), .SDcols = setdiff(names(dt_persistencia), "TIMESTAMP")]
    dt_persistencia[, hora := hour(TIMESTAMP)]
    dt_persistencia[, diurno := hora <= 5 | hora >= 18]
    dt_persistencia[, hora := NULL]
    dt_persistencia[, Bandera_persistencia := fifelse(is.na(get(variable_analizada)), "ND",
                                                      fifelse(diurno, "ND",
                                                              fifelse(Todos_Iguales, "D", "C")))]
    inds = which(dt_persistencia$Bandera_persistencia == "D")
    secuencias = unlist(lapply(inds, function(indice) {
      if (indice > n_persistencias) {
        return((indice - n_persistencias):(indice - 1))
      } else {
        return(integer(0))  # Retornar un vector vacío si el índice no permite una secuencia completa
      }
    }))
    if (length(inds) > 0) {
      dt_persistencia[secuencias, Bandera_persistencia := "D"]
    }
    dt_persistencia[, c((col_names), (variable_analizada)) := NULL]
    dt_persistencia[, diurno := NULL]
    dt_persistencia[, Todos_Iguales := NULL]

  } else {
    # Aplicar la función a cada fila
    dt_persistencia[, Todos_Iguales := apply(.SD, 1, todos_iguales), .SDcols = setdiff(names(dt_persistencia), "TIMESTAMP")]
    dt_persistencia[, Bandera_persistencia := fifelse(is.na(get(variable_analizada)), "ND",
                                                      fifelse(Todos_Iguales, "D", "C"))]

    inds = which(dt_persistencia$Bandera_persistencia == "D")
    if (length(inds) > 0) {
      secuencias = unlist(lapply(inds, function(indice) {
        if (indice > n_persistencias) {
          return((indice - n_persistencias):(indice - 1))
        } else {
          return(integer(0))  # Retornar un vector vacío si el índice no permite una secuencia completa
        }
      }))
      dt_persistencia[secuencias, Bandera_persistencia := "D"]
    }
    dt_persistencia[, c((col_names), (variable_analizada)) := NULL]
    dt_persistencia[, Todos_Iguales := NULL]
  } # cierro el else.
  return(dt_persistencia)
}

# 2.3 Test de coherencia.
Test_coherencia = function(dt, variable_analizada, ventana) {
  dt_coherencia = copy(dt)
  col_promedio = paste0(variable_analizada, "_promedio")
  col_desviacion = paste0(variable_analizada, "_desviacion")

  dt_coherencia[, (col_promedio) := frollmean(get(variable_analizada), n = ventana, align = "right")]
  dt_coherencia[, (col_desviacion) := frollapply(get(variable_analizada), n = ventana, sd, align = "right")]

  dt_coherencia[, Bandera_coherencia := {
    ifelse(is.na(get(variable_analizada)), "ND",
           ifelse(!is.na(get(col_promedio)) & !is.na(get(col_desviacion)),
                  ifelse(get(variable_analizada) >= (get(col_promedio) - 3 * get(col_desviacion)) &
                           get(variable_analizada) <= (get(col_promedio) + 3 * get(col_desviacion)),
                         "C", "D"),
                  "ND"))
  }]

  dt_coherencia[, c((col_promedio), (col_desviacion), (variable_analizada)) := NULL]
  return(dt_coherencia)
}

###################### Test específicos para cada variable #####################
# Test para velocidad del viento.
Test_VD_viento = function(dt) {
  setnames(dt, c("TIMESTAMP", "Velocidad", "Dirección"))

  if (ncol(dt) != 3) {
    stop("La longidud del dt no es correcta. Se esperaba: c('Fecha', 'Velocidad', 'Dirección'")
  }
  dt_viento = dt[, Bandera_DV_viento := fifelse(is.na(Velocidad) | is.na(Dirección), "ND",
                                                fifelse(Velocidad > 0 & Dirección > 0, "C",
                                                        fifelse(Velocidad == 0 & Dirección == 0, "C", "D")))]

  dt_viento = dt_viento[, .(TIMESTAMP, Bandera_DV_viento)]
  return(dt_viento)
}
