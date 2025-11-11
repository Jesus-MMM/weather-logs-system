#!/usr/bin/env python3
"""
API REST para Consulta de Logs Meteorológicos y Reportes
FastAPI con endpoints para consultas históricas y dashboards
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import os
from dotenv import load_dotenv
import pandas as pd
from models import *

# Configuración
load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("weather_api")

# FastAPI App
app = FastAPI(
    title="API Meteorológica - Consultas y Reportes",
    description="API REST para consulta de logs históricos y generación de reportes meteorológicos",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database Connection
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB', 'weather_logs'),
            user=os.getenv('POSTGRES_USER', 'admin'),
            password=os.getenv('POSTGRES_PASSWORD', 'password'),
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        logger.error(f"Error conectando a PostgreSQL: {e}")
        raise HTTPException(status_code=500, detail="Error de conexión a la base de datos")

# Endpoints
@app.get("/", response_model=HealthCheck)
async def root():
    """Health check del sistema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar conexión y contar registros
        cursor.execute("SELECT COUNT(*) as total FROM registros_meteorologicos")
        total_registros = cursor.fetchone()['total']
        
        cursor.close()
        conn.close()
        
        return HealthCheck(
            status="active",
            timestamp=datetime.now(),
            version="1.0.0",
            database=True,
            total_registros=total_registros
        )
    except Exception as e:
        return HealthCheck(
            status="degraded",
            timestamp=datetime.now(),
            version="1.0.0",
            database=False,
            total_registros=0
        )

@app.get("/registros", response_model=List[RegistroMeteorologicoResponse])
async def obtener_registros(
    id_estacion: Optional[str] = Query(None, description="Filtrar por estación"),
    fecha_desde: Optional[date] = Query(None, description="Fecha desde (YYYY-MM-DD)"),
    fecha_hasta: Optional[date] = Query(None, description="Fecha hasta (YYYY-MM-DD)"),
    calidad_dato: Optional[str] = Query(None, description="Filtrar por calidad de dato"),
    limite: int = Query(100, ge=1, le=1000, description="Límite de registros")
):
    """Obtener registros meteorológicos históricos con filtros"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Construir query dinámica
        query = """
            SELECT id, id_estacion, nombre_estacion, ubicacion,
                   temperatura, humedad, presion, velocidad_viento,
                   direccion_viento, precipitacion, radiacion_solar,
                   condicion_meteorologica, fecha_medicion, calidad_dato
            FROM registros_meteorologicos
            WHERE 1=1
        """
        params = []
        
        if id_estacion:
            query += " AND id_estacion = %s"
            params.append(id_estacion)
            
        if fecha_desde:
            query += " AND DATE(fecha_medicion) >= %s"
            params.append(fecha_desde)
            
        if fecha_hasta:
            query += " AND DATE(fecha_medicion) <= %s"
            params.append(fecha_hasta)
            
        if calidad_dato:
            query += " AND calidad_dato = %s"
            params.append(calidad_dato)
            
        query += " ORDER BY fecha_medicion DESC LIMIT %s"
        params.append(limite)
        
        cursor.execute(query, params)
        registros = cursor.fetchall()
        
        cursor.close()
        conn.close()
        logger.info(f"Consulta registros: {len(registros)} resultados")
        return registros
        
    except Exception as e:
        logger.error(f"Error en /registros: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.get("/estaciones/{id_estacion}/estadisticas", response_model=EstadisticasEstacion)
async def obtener_estadisticas_estacion(
    id_estacion: str,
    periodo: PeriodoReporte = PeriodoReporte.ULTIMO_MES
):
    """Obtener estadísticas detalladas de una estación específica"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Calcular fechas según periodo
        fecha_hasta = date.today()
        if periodo == PeriodoReporte.HOY:
            fecha_desde = fecha_hasta
        elif periodo == PeriodoReporte.ULTIMA_SEMANA:
            fecha_desde = fecha_hasta - timedelta(days=7)
        elif periodo == PeriodoReporte.ULTIMO_MES:
            fecha_desde = fecha_hasta - timedelta(days=30)
        else:
            fecha_desde = fecha_hasta - timedelta(days=30)  # Default
        
        query = """
            SELECT 
                id_estacion,
                nombre_estacion,
                COUNT(*) as total_registros,
                ROUND(AVG(temperatura)::numeric, 2) as temperatura_promedio,
                MIN(temperatura) as temperatura_minima,
                MAX(temperatura) as temperatura_maxima,
                ROUND(AVG(humedad)::numeric, 2) as humedad_promedio,
                MAX(velocidad_viento) as velocidad_viento_maxima,
                SUM(precipitacion) as precipitacion_total
            FROM registros_meteorologicos
            WHERE id_estacion = %s 
                AND DATE(fecha_medicion) BETWEEN %s AND %s
                AND calidad_dato = 'valido'
            GROUP BY id_estacion, nombre_estacion
        """
        
        cursor.execute(query, (id_estacion, fecha_desde, fecha_hasta))
        resultado = cursor.fetchone()
        
        if not resultado:
            raise HTTPException(status_code=404, detail="Estación no encontrada o sin datos")
        
        cursor.close()
        conn.close()
        
        return EstadisticasEstacion(**resultado)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en /estadisticas: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.get("/reportes/diario", response_model=List[ReporteDiario])
async def obtener_reporte_diario(
    dias: int = Query(7, ge=1, le=30, description="Número de días a reportar")
):
    """Generar reporte diario de las últimas N días"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                DATE(fecha_medicion) as fecha,
                COUNT(*) as total_registros,
                ROUND(AVG(temperatura)::numeric, 2) as temperatura_promedio,
                ROUND(AVG(humedad)::numeric, 2) as humedad_promedio,
                SUM(precipitacion) as precipitacion_total
            FROM registros_meteorologicos
            WHERE fecha_medicion >= CURRENT_DATE - INTERVAL '%s days'
                AND calidad_dato = 'valido'
            GROUP BY DATE(fecha_medicion)
            ORDER BY fecha DESC
        """
        
        cursor.execute(query, (dias,))
        reportes = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return reportes
        
    except Exception as e:
        logger.error(f"Error en /reportes/diario: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.get("/alertas/pendientes", response_model=List[AlertasResponse])
async def obtener_alertas_pendientes():
    """Obtener alertas meteorológicas pendientes de confirmación"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT a.*, e.nombre_estacion
            FROM alertas_meteorologicas a
            LEFT JOIN estaciones_meteorologicas e ON a.id_estacion = e.id_estacion
            WHERE a.esta_confirmada = FALSE
            ORDER BY a.fecha_activacion DESC
        """
        
        cursor.execute(query)
        alertas = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return alertas
        
    except Exception as e:
        logger.error(f"Error en /alertas/pendientes: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.get("/estaciones")
async def listar_estaciones():
    """Listar todas las estaciones meteorológicas"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id_estacion, nombre_estacion, nombre_ubicacion, 
                   latitud, longitud, altitud, tipo_estacion, esta_activa
            FROM estaciones_meteorologicas
            ORDER BY nombre_estacion
        """)
        estaciones = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {"estaciones": estaciones}
        
    except Exception as e:
        logger.error(f"Error en /estaciones: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.get("/dashboard/metricas")
async def obtener_metricas_dashboard():
    """Obtener métricas para dashboard en tiempo real"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Métricas generales
        cursor.execute("""
            SELECT 
                COUNT(*) as total_registros,
                COUNT(DISTINCT id_estacion) as total_estaciones,
                ROUND(AVG(temperatura)::numeric, 2) as temperatura_global,
                SUM(precipitacion) as precipitacion_total
            FROM registros_meteorologicos
            WHERE fecha_medicion >= CURRENT_DATE
                AND calidad_dato = 'valido'
        """)
        metricas = cursor.fetchone()
        
        # Últimas mediciones
        cursor.execute("""
            SELECT id_estacion, temperatura, humedad, fecha_medicion
            FROM registros_meteorologicos
            WHERE calidad_dato = 'valido'
            ORDER BY fecha_medicion DESC
            LIMIT 10
        """)
        ultimas_mediciones = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "metricas_globales": metricas,
            "ultimas_mediciones": ultimas_mediciones,
            "timestamp_consulta": datetime.now()
        }
        
    except Exception as e:
        logger.error(f"Error en /dashboard/metricas: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

# Startup/shutdown events
@app.on_event("startup")
async def startup_event():
    logger.info("API Meteorológica de Consultas iniciada")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("API Meteorológica de Consultas detenida")