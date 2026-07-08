"""
Servicio para normalizar datos del libro diario antes de insertar en DB
"""
import pandas as pd
import numpy as np


class Normalizer:
    """Normaliza datos limpios para inserción en la base de datos"""

    COLUMNAS_DB = [
        'id_empresa', 'fecha_asiento', 'tipo_asiento', 'nro_asiento', 'nro_renglon',
        'codigo_cuenta', 'descripcion_cuenta', 'descripcion_movimiento',
        'tipo_subcta', 'nro_subcuenta', 'tipo_comprobante', 'sucursal',
        'nro_comprobante', 'nombre_tercero', 'debe', 'haber',
        'periodo_anio', 'periodo_mes', 'fecha_carga_original',
        'descripcion_asiento', 'referencia'
    ]

    # Columnas de texto que pueden contener tabs/newlines que rompen COPY
    COLUMNAS_TEXTO = [
        'descripcion_cuenta', 'descripcion_movimiento',
        'descripcion_asiento', 'nombre_tercero',
        'referencia', 'nro_comprobante'
    ]

    @staticmethod
    def normalizar_para_db(df: pd.DataFrame, id_empresa: int) -> pd.DataFrame:
        df = df.copy()
        df['id_empresa'] = id_empresa

        # Numéricos obligatorios
        df['debe']  = pd.to_numeric(df['debe'],  errors='coerce').fillna(0.0)
        df['haber'] = pd.to_numeric(df['haber'], errors='coerce').fillna(0.0)

        # codigo_cuenta como int
        df['codigo_cuenta'] = pd.to_numeric(
            df['codigo_cuenta'], errors='coerce'
        ).astype('Int64')

        # Campos de texto opcionales → limpiar y convertir vacíos a None
        for col in Normalizer.COLUMNAS_TEXTO:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace('\t', ' ', regex=False)   # tabs rompen COPY
                    .str.replace('\n', ' ', regex=False)   # newlines rompen COPY
                    .str.replace('\r', ' ', regex=False)   # carriage return
                    .str.strip()
                )
                # Vacíos y 'None' literal → None real
                df[col] = df[col].replace({'': None, 'None': None, 'nan': None})

        # Numéricos opcionales → None si 0 o NaN
        for col in ['tipo_subcta', 'nro_subcuenta', 'tipo_comprobante', 'sucursal']:
            if col in df.columns:
                df[col] = df[col].replace({0: None, pd.NA: None})
                if str(df[col].dtype) == 'Int64':
                    df[col] = df[col].where(df[col].notna(), None)

        return df[Normalizer.COLUMNAS_DB]