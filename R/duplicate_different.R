duplicate_different = function(dt, variable_analizada, ventana, num_cores = parallel::detectCores() - 1) {
  message("Calculando el valor correcto, porfavor espere..........")
  # Configurar el plan de paralelización
  plan(multisession, workers = num_cores)

  # Crear una copia de la tabla para trabajar sin alterar el original
  dt_copia = copy(dt)
  setkey(dt_copia, TIMESTAMP)

  # Función para calcular estadísticas
  calcular_estadisticas = function(x) {
    media = mean(x, na.rm = TRUE)
    mediana = median(x, na.rm = TRUE)
    desv = sd(x, na.rm = TRUE)

    # Límites basados en la media y la desviación estándar
    Li_media = media - 2 * desv
    Ls_media = media + 2 * desv

    # Límites basados en la mediana y la desviación estándar
    Li_mediana = mediana - 2 * desv
    Ls_mediana = mediana + 2 * desv

    list(
      Li_media = Li_media,
      Ls_media = Ls_media,
      Li_mediana = Li_mediana,
      Ls_mediana = Ls_mediana,
      desv = desv
    )
  }

  # Función para contar éxitos
  contar_exitos = function(valor, stats) {
    count_media = ifelse(valor >= stats$Li_media & valor <= stats$Ls_media, 1, 0)
    count_mediana = ifelse(valor >= stats$Li_mediana & valor <= stats$Ls_mediana, 1, 0)

    # Suma los éxitos que cumplan las condiciones
    total_exitos = count_media + count_mediana
    return(total_exitos)
  }

  # Función para procesar un timestamp duplicado
  procesar_timestamp = function(ts, dt, variable_analizada, ventana) {
    ventana_inicio = ts - minutes(ventana)

    # Calcular estadísticas de la ventana anterior
    stats = dt[TIMESTAMP >= ventana_inicio & TIMESTAMP < ts,
                calcular_estadisticas(.SD[[variable_analizada]])]

    # Evaluar valores duplicados
    resultados = dt[TIMESTAMP == ts, .(
      value = .SD[[variable_analizada]],
      n_exitos = contar_exitos(.SD[[variable_analizada]], stats)
    )]

    # Determinar el valor real
    if (length(unique(resultados$n_exitos)) == 1) {
      valor_real = mean(resultados$value[resultados$n_exitos == max(resultados$n_exitos)], na.rm = TRUE)
    } else {
      valor_real = resultados[which.max(n_exitos), value]
    }

    list(ts = ts, valor_real = valor_real)
  }

  # Procesar duplicados
  print("Iniciando cálculos necesarios....")
  dup_totales = length(dt_copia[, duplicated(TIMESTAMP)])
  while (dt_copia[, any(duplicated(TIMESTAMP))]) {
    # Identificar timestamps duplicados
    dup_timestamps = dt_copia[, .N, by = TIMESTAMP][N > 1, TIMESTAMP]

    # Procesar cada timestamp duplicado en paralelo
    resultados = future_lapply(dup_timestamps, function(ts) {
      procesar_timestamp(ts, dt_copia, variable_analizada, ventana)
    }, future.seed = TRUE)

    # Actualizar valores en la tabla
    for (res in resultados) {
      dt_copia[TIMESTAMP == res$ts, (variable_analizada) := res$valor_real]
    }

    # Eliminar duplicados
    dt_copia = unique(dt_copia, by = "TIMESTAMP")
    dups_restantes = length(dt_copia[, duplicated(TIMESTAMP)])
  }

  # Cerrar el plan de paralelización
  plan(sequential)
  return(dt_copia)
}
