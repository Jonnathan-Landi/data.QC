#'  Aggregate Data by Time Interval
#'
#' This function aggregates data from a `data.table` based on a specified time unit and applies various aggregation functions (e.g., mean, min, max) to different variables.
#'
#' @param dt A `data.table` containing the data to be aggregated. The `dt` must include a `TIMESTAMP` column.
#' @param variable The name of the variable to be aggregated. The function will apply the aggregation method specified in `typ_fun` or default to the pre-defined method for that variable.
#' @param x_min (Optional) Minimum number of non-missing data points required for the aggregation. Defaults to `1` if not specified.
#' @param by A string indicating the time unit for aggregation (e.g., "hour", "day").
#' @param typ_fun (Optional) A custom aggregation function to be applied. If not specified, the function uses a default aggregation function based on the variable name.
#'
#' @return A `data.table` containing the aggregated data, with a `TIMESTAMP` column and aggregated values for the specified variable.
#' @import data.table
#' @importFrom lubridate floor_date
#' @examples
#' TIMESTAMP <- seq.POSIXt(as.POSIXct("2024-09-01 00:00:00"),
#' as.POSIXct("2024-10-01 01:00:00"),
#' by = "5 min")
#' TempAire_Avg <- runif(length(TIMESTAMP), 20, 50)
#' dt <- data.table(TIMESTAMP = TIMESTAMP, TempAire_Avg = TempAire_Avg)
#' data_pooling(dt, variable = "TempAire_Avg", x_min = 1, by = "hour")
#' @export

################################################################################

data_pooling = function(dt, variable, x_min = NULL, by, typ_fun = NULL){
  # tipo de agrupación
  inicio_hora = floor_date(min(dt$TIMESTAMP), unit = by)
  fin_hora = floor_date(max(dt$TIMESTAMP), unit = by)

  # Crear secuencia desde el tiempo redondeado hasta el máximo tiempo
  secuencia_ideal = data.table(TIMESTAMP = seq.POSIXt(from = inicio_hora,
                                                      to = fin_hora,
                                                      by = by))
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
    Lluvia_Tot = "sum",
    level_Min = "min",
    level_Avg = "mean",
    level_Max = "max"
  )
  message("agrupando datos de:....")
  for (variable in setdiff(names(dt), "TIMESTAMP")) {
    dt_temp = dt[, .(TIMESTAMP, get(variable))]
    setnames(dt_temp, c("TIMESTAMP", variable))
    if (variable %in% names(fun_agrup)) {
      fun_apli = ifelse(is.null(typ_fun), fun_agrup[[variable]], typ_fun)
      message(paste0("función aplicada: ", fun_apli))
      print(variable)

      fun_apli = match.fun(fun_apli)

      # minimo de datos a considerar
      x_min = ifelse(is.null(x_min), 1, x_min)

      dt_temp = dt_temp[, .(
        variable = if (sum(!is.na(get(variable))) >= x_min) {
          fun_apli(get(variable), na.rm = TRUE)
        } else {
          NA_real_
        }
      ), by = .(TIMESTAMP = floor_date(TIMESTAMP, by))]

      setnames(dt_temp, c("TIMESTAMP", variable))

      dt_temp[is.infinite(get(variable)), (variable) := NA_real_]
      dt_temp[is.nan(get(variable)), (variable) := NA_real_]
      dt_hourly = merge(dt_hourly, dt_temp, by = "TIMESTAMP", all = TRUE)
    } else {
      stop("variable no encontrada")
    }
  }

  c1 = nrow(secuencia_ideal)
  c2 = nrow(dt_hourly)
  if (c1 != c2) {
    stop("Test failed: El numero de filas no coincide con el set de datos original")
  }

  # ELIMINO COLUMNAS
  cols_to_check = setdiff(names(dt_hourly), "TIMESTAMP")

  # Verificar si todos los valores en las columnas seleccionadas de la primera fila son NA
  if (all(is.na(dt_hourly[1, ..cols_to_check]))) {
    dt_hourly = dt_hourly[-1, ]
  }

  if (all(is.na(dt_hourly[nrow(dt_hourly), ..cols_to_check]))) {
    # Eliminar la última fila si la condición se cumple
    dt_hourly = dt_hourly[-nrow(dt_hourly), ]
  }
  return(dt_hourly)
}
