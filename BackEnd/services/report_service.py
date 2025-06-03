from datetime import datetime
from database.connection import get_db_connection

def generate_report(fecha_inicio, fecha_fin, empresa=None):
    """
    Genera un reporte resumido usando DATE(fecha_creacion) para filtrar.
    - total_pagado: suma de todos los montos no nulos.
    - total_cuentas_cobradas: cuenta sólo los pagos con monto no nulo.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Convertir strings a date si es necesario
    if isinstance(fecha_inicio, str):
        fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
    if isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').date()

    if empresa:
        sql = """
            SELECT
                COALESCE(SUM(monto), 0)::float AS total_pagado,
                COALESCE(COUNT(monto), 0)    AS total_cuentas
            FROM pagos
            WHERE empresa = %s
              AND DATE(fecha_creacion) BETWEEN %s AND %s
        """
        params = (empresa, fecha_inicio, fecha_fin)
    else:
        sql = """
            SELECT
                COALESCE(SUM(monto), 0)::float AS total_pagado,
                COALESCE(COUNT(monto), 0)    AS total_cuentas
            FROM pagos
            WHERE DATE(fecha_creacion) BETWEEN %s AND %s
        """
        params = (fecha_inicio, fecha_fin)

    cur.execute(sql, params)
    total_pagado, total_cuentas = cur.fetchone()

    cur.close()
    conn.close()

    return {
        'total_pagado': float(total_pagado),
        'total_cuentas_cobradas': int(total_cuentas)
    }


def get_empresas():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT empresa FROM pagos")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r[0] for r in rows]


def search_pagos(empresa, fecha_inicio, fecha_fin):
    """
    Devuelve lista de pagos completos filtrados por empresa y fecha_creacion.
    Sólo trae entradas cuyo monto NO sea NULL.
    """
    if isinstance(fecha_inicio, str):
        fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
    if isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d')

    conn = get_db_connection()
    cur = conn.cursor()

    base_sql = """
        SELECT dni, oponente, monto, empresa, fecha_pago, cuotas 
        FROM pagos
        WHERE fecha_creacion BETWEEN %s AND %s
          AND monto IS NOT NULL
    """
    if empresa:
        sql = base_sql.replace("WHERE", "WHERE empresa = %s AND")
        params = (empresa, fecha_inicio, fecha_fin)
    else:
        sql = base_sql
        params = (fecha_inicio, fecha_fin)

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            'dni': dni,
            'oponente': oponente,
            'monto': float(monto),
            'empresa': empresa_,
            'fecha_pago': fecha_pago,
            'cuotas': cuotas
        }
        for dni, oponente, monto, empresa_, fecha_pago, cuotas in rows
    ]

def get_monthly_summary(fecha_inicio, fecha_fin, empresa=None):
    """
    Devuelve una lista de dicts con:
      - mes: 'YYYY-MM'
      - total_pagado: suma de montos en ese mes
      - total_cuentas: número de pagos en ese mes
    Filtra por fecha_creacion entre fecha_inicio y fecha_fin.
    """
    # convertir strings a date
    if isinstance(fecha_inicio, str):
        fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
    if isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d').date()

    conn = get_db_connection()
    cur = conn.cursor()

    # base SQL para agrupar por mes
    sql = """
        SELECT
          to_char(DATE(fecha_creacion), 'YYYY-MM') AS mes,
          COALESCE(SUM(monto),0)::float    AS total_pagado,
          COALESCE(COUNT(monto),0)         AS total_cuentas
        FROM pagos
        WHERE DATE(fecha_creacion) BETWEEN %s AND %s
    """
    params = [fecha_inicio, fecha_fin]

    # si filtramos por empresa
    if empresa:
        sql += "  AND empresa = %s"
        params.append(empresa)

    sql += " GROUP BY mes ORDER BY mes;"

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            'mes': mes,
            'total_pagado': total_pagado,
            'total_cuentas': total_cuentas
        }
        for mes, total_pagado, total_cuentas in rows
    ]
