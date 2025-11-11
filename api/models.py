from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date
from enum import Enum

class PeriodoReporte(str, Enum):
    HOY = "hoy"
    ULTIMA_SEMANA = "ultima_semana"
    ULTIMO_MES = "ultimo_mes"
    PERSONALIZADO = "personalizado"

class FiltrosConsulta(BaseModel):
    id_estacion: Optional[str] = None
    fecha_desde: Optional[date] = None
    fecha_hasta: Optional[date] = None
    calidad_dato: Optional[str] = None
    limite: int = Field(100, ge=1, le=1000)

class RegistroMeteorologicoResponse(BaseModel):
    id: int
    id_estacion: str
    nombre_estacion: Optional[str]
    ubicacion: Optional[str]
    temperatura: Optional[float]
    humedad: Optional[int]
    presion: Optional[float]
    velocidad_viento: Optional[float]
    direccion_viento: Optional[int]
    precipitacion: Optional[float]
    radiacion_solar: Optional[float]
    condicion_meteorologica: Optional[str]
    fecha_medicion: datetime
    calidad_dato: str

class EstadisticasEstacion(BaseModel):
    id_estacion: str
    nombre_estacion: Optional[str]
    total_registros: int
    temperatura_promedio: Optional[float]
    temperatura_minima: Optional[float]
    temperatura_maxima: Optional[float]
    humedad_promedio: Optional[float]
    velocidad_viento_maxima: Optional[float]
    precipitacion_total: Optional[float]

class ReporteDiario(BaseModel):
    fecha: date
    total_registros: int
    temperatura_promedio: Optional[float]
    humedad_promedio: Optional[float]
    precipitacion_total: Optional[float]

class AlertasResponse(BaseModel):
    id: int
    id_estacion: str
    nombre_estacion: Optional[str]
    tipo_alerta: str
    valor_umbral: float
    valor_actual: float
    severidad: str
    mensaje_alerta: str
    fecha_activacion: datetime
    esta_confirmada: bool

class HealthCheck(BaseModel):
    status: str
    timestamp: datetime
    version: str
    database: bool
    total_registros: int