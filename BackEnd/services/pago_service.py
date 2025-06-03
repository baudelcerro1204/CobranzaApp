import os
import pandas as pd
import io
import hashlib
import logging
from pydantic import ValidationError

from database.connection import get_db_connection
from models.schemas import PagoSchema
from models.pago import Pago

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_spreadsheet(file_input):
    """
    Acepta una ruta de archivo (str) o un FileStorage (de Flask).
    Devuelve un DataFrame de pandas.
    """
    # Si es un FileStorage de Flask
    if hasattr(file_input, "filename"):
        filename = file_input.filename
        ext = os.path.splitext(filename)[1].lower()
        data = file_input.read()
        buf = io.BytesIO(data)

        if ext in (".xls", ".xlsx"):
            return pd.read_excel(buf)
        if ext == ".csv":
            return pd.read_csv(buf)
        if ext == ".ods":
            return pd.read_excel(buf, engine="odf")

        raise ValueError(f"Formato no soportado: {ext}")

    # Si es una ruta en disco
    ext = os.path.splitext(file_input)[1].lower()
    if ext in (".xls", ".xlsx"):
        return pd.read_excel(file_input)
    if ext == ".csv":
        return pd.read_csv(file_input)
    if ext == ".ods":
        return pd.read_excel(file_input, engine="odf")

    raise ValueError(f"Formato no soportado: {ext}")


def get_existing_hashes():
    """Carga todos los hashes de pagos ya en base de datos."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT dni, monto, empresa, fecha_pago, cuotas FROM pagos")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    hashes = set()
    for dni, monto, empresa, fecha_pago, cuotas in rows:
        raw = f"{str(dni).strip()}|{round(float(monto),2):.2f}|{str(empresa).strip()}|{fecha_pago or ''}|{cuotas or ''}"
        hashes.add(hashlib.sha256(raw.encode("utf-8")).hexdigest())
    return hashes


def bulk_insert_pagos(cursor, pagos):
    """Inserta en la base todos los pagos nuevos de una vez (incluye record_hash)."""
    sql = """
    INSERT INTO pagos
      (dni, oponente, monto, empresa, fecha_pago, cuotas, record_hash)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    data = [p.to_tuple() for p in pagos]
    cursor.executemany(sql, data)


def process_file(file_input):
    """
    Procesa XLSX/CSV/ODS, saltea filas con monto nulo/NaN, valida con Pydantic,
    evita duplicados y retorna lista de Pago.
    """
    df = read_spreadsheet(file_input)
    df.columns = df.columns.str.lower().str.strip()

    required = {"dni", "monto", "empresa"}
    if not required.issubset(df.columns):
        faltan = required - set(df.columns)
        raise ValueError(f"Faltan columnas: {', '.join(faltan)}")

    existing_hashes = get_existing_hashes()
    logging.info(f"{len(existing_hashes)} pagos existentes cargados (por hash).")

    nuevos = []
    seen = set()

    for idx, row in df.iterrows():
        raw_monto = row.get("monto")
        # 1) Skip si el monto es NaN, None o no es convertible a float
        if pd.isna(raw_monto):
            logging.info(f"Fila {idx} ignorada: monto nulo/NaN.")
            continue
        try:
            monto_val = float(raw_monto)
        except Exception:
            logging.info(f"Fila {idx} ignorada: monto no numérico ({raw_monto}).")
            continue

        datos = {
            "dni": str(row.get("dni")).strip(),
            "monto": monto_val,
            "empresa": row.get("empresa"),
            "oponente": row.get("oponente", None),
            "fecha_pago": row.get("fecha_de_pago", None),
            "cuotas": row.get("cuotas", None),
        }
        try:
            ps = PagoSchema(**datos)
        except ValidationError as ve:
            logging.warning(f"Fila {idx} inválida tras validación: {ve}")
            continue

        h = ps.record_hash
        if h in existing_hashes:
            logging.info(f"Fila {idx} ya existe (hash duplicado).")
            continue
        if h in seen:
            logging.info(f"Fila {idx} duplicada en archivo.")
            continue

        seen.add(h)
        nuevos.append(
            Pago(
                dni=ps.dni,
                oponente=ps.oponente,
                monto=ps.monto,
                empresa=ps.empresa,
                fecha_pago=ps.fecha_pago,
                cuotas=ps.cuotas,
                record_hash=ps.record_hash,
            )
        )

    logging.info(f"{len(nuevos)} pagos nuevos listos para insertar.")
    if not nuevos:
        logging.warning("No hay pagos nuevos.")
        return []

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        bulk_insert_pagos(cur, nuevos)
        conn.commit()
        logging.info("Pagos insertados exitosamente.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error al insertar pagos: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return nuevos