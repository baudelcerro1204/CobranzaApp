# services/report_service.py
from datetime import datetime
from database.connection import get_db_connection

def generate_report(fecha_inicio, fecha_fin, empresa=None):
    connection = get_db_connection()
    cursor = connection.cursor()

    # Asegurarse de que las fechas sean objetos datetime
    if isinstance(fecha_inicio, str):
        fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
    if isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d')

    # Consultar el monto total y el total de pagos entre las fechas dadas
    if empresa:
        cursor.execute("""
            SELECT SUM(monto), COUNT(*) 
            FROM pagos
            WHERE empresa = %s AND fecha_creacion >= %s AND fecha_creacion <= %s
        """, (empresa, fecha_inicio, fecha_fin))
    else:
        cursor.execute("""
            SELECT SUM(monto), COUNT(*) 
            FROM pagos
            WHERE fecha_creacion >= %s AND fecha_creacion <= %s
        """, (fecha_inicio, fecha_fin))

    result = cursor.fetchone()
    total_pagado = result[0] if result[0] else 0
    total_cuentas_cobradas = result[1] if result[1] else 0

    cursor.close()
    connection.close()

    return {'total_pagado': total_pagado, 'total_cuentas_cobradas': total_cuentas_cobradas}

def get_empresas():
    connection = get_db_connection()
    cursor = connection.cursor()

    # Obtener todas las empresas sin duplicados
    cursor.execute("SELECT DISTINCT empresa FROM pagos")
    empresas = cursor.fetchall()

    cursor.close()
    connection.close()

    # Convertir el resultado en una lista de empresas
    return [empresa[0] for empresa in empresas]

def search_pagos(empresa, fecha_inicio, fecha_fin):
    # Convertir las fechas de cadena a objeto datetime
    try:
        fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d')
    except ValueError:
        return {"error": "El formato de las fechas es incorrecto, debe ser YYYY-MM-DD"}

    connection = get_db_connection()
    cursor = connection.cursor()

    # Filtrar por empresa si se proporciona
    if empresa:
        cursor.execute("""
            SELECT * FROM pagos
            WHERE empresa = %s AND fecha_creacion >= %s AND fecha_creacion <= %s
        """, (empresa, fecha_inicio, fecha_fin))
    else:
        cursor.execute("""
            SELECT * FROM pagos
            WHERE fecha_creacion >= %s AND fecha_creacion <= %s
        """, (fecha_inicio, fecha_fin))

    # Obtener los resultados de la consulta
    pagos = cursor.fetchall()

    cursor.close()
    connection.close()

    # Convertir los resultados en una lista de diccionarios
    pagos_lista = []
    for pago in pagos:
        pagos_lista.append({
            'dni': pago[0],
            'oponente': pago[1],
            'monto': pago[2],
            'empresa': pago[3],
            'fecha_pago': pago[4],
            'cuotas': pago[5]
        })

    return pagos_lista
