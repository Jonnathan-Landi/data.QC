################################################################################
#                           PCCM Algorithm
################################################################################
# EJECUTAR UNA SOLA VEZ ESTA PARTE DEL SCRIPT
# Librerías necesarias 
library(pacman)
p_load(data.table, dplyr, svDialogs, lubridate, ggplot2, 
       gridExtra, zoo, sp, spdep, trend, this.path, forecast, sf,
       future.apply, doParallel,openxlsx)

# Complementos indispensables 
dircs = this.path()
dir_Base = dirname(dircs)
dir_Base = paste0(dir_Base, "/PCCM_Base.R")
env_P = new.env()
sys.source(dir_Base, envir = env_P)
################################################################################
#                     parámetros modificables
################################################################################
directory = "C:/Users/Jonna/Desktop/Proyecto_U/Base de Datos/DATOS_2024-08-29/Meteorológica"
nombre.estat = "CebollarPTAPM"
################################################################################
#                   Control de Calidad 5 minutos
################################################################################
datos = env_P$leer.datos(directory, nombre.estat)
datos_5minutos = env_P$minutes_control(datos)
################################################################################
#                     Control de Calidad horario
################################################################################
datos_horarios = env_P$hourly_pooling(datos_5minutos)
control_horario = env_P$hourly_control(datos_horarios)
datos_horarios = env_P$corregir_datos(control_horario, datos_horarios)
