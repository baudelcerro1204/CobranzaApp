import pandas as pd
from models.pago import Pago
from database.connection import get_db_connection
from datetime import datetime
import logging

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_excel(file):
    # Leer el archivo Excel en memoria
    df = pd.read_excel(file)

    # Normalizar los nombres de las columnas a minúsculas
    df.columns = df.columns.str.lower()

    # Conectar a la base de datos
    connection = get_db_connection()
    cursor = connection.cursor()

    pagos_no_duplicados = []

    # Iterar sobre cada fila del DataFrame
    for _, row in df.iterrows():
        try:
            logging.info(f"Procesando fila con DNI: {row['dni']} y Monto: {row['monto']}")

            # Verificar si 'dni' o 'empresa' están vacíos (None o NaN)
            if pd.isna(row['dni']) or pd.isna(row['empresa']):
                logging.error(f"Faltan datos obligatorios (DNI o Empresa) en la fila con DNI: {row['dni']} y Empresa: {row['empresa']}")
                connection.rollback()  # Cancelar toda la operación si hay datos faltantes en 'dni' o 'empresa'
                return []

            # Asignar None a 'fecha_pago' y 'cuotas' si están vacíos o NaN
            fecha_pago = None if pd.isna(row.get('fecha_de_pago', None)) else row['fecha_de_pago']
            cuotas = None if pd.isna(row.get('cuotas', None)) else row['cuotas']

            pago = Pago(
                dni=row['dni'],
                oponente=row.get('oponente', None),
                monto=row['monto'],
                empresa=row['empresa'],
                fecha_pago=fecha_pago,
                cuotas=cuotas
            )

            # Verificar si el pago ya existe en la base de datos
            if not check_duplicate_payment(pago.dni, pago.monto, pago.empresa):
                # Mostrar los datos antes de insertarlos
                logging.info(f"Datos que se van a guardar: {pago.to_dict()}")
                try:
                    pago.save_to_db(cursor)
                    pagos_no_duplicados.append(pago)
                except Exception as e:
                    logging.error(f"Error al guardar el pago con DNI: {pago.dni} y Monto: {pago.monto}. Error: {e}")
                    connection.rollback()  # Si ocurre un error, revertir toda la transacción
                    return []

        except Exception as e:
            logging.error(f"Error general al procesar la fila con DNI: {row['dni']}. Error: {e}")
            connection.rollback()  # Si ocurre un error, revertir toda la transacción
            return []

    # Confirmar la transacción y cerrar la conexión
    connection.commit()
    cursor.close()
    connection.close()

    return pagos_no_duplicados





def save_to_db(self, cursor):
    try:
        # Asegurarse de que el DNI se maneje como texto
        dni_text = str(self.dni)

        # Asegurarse de que 'fecha_pago' y 'cuotas' sean None si no tienen valor
        fecha_pago = None if self.fecha_pago is None or pd.isna(self.fecha_pago) else self.fecha_pago
        cuotas = None if self.cuotas is None or pd.isna(self.cuotas) else self.cuotas

        # Mostrar los datos antes de la inserción
        logging.info(f"Intentando insertar el pago con los siguientes datos:")
        logging.info(f"DNI: {dni_text}, Oponente: {self.oponente}, Monto: {self.monto}, Empresa: {self.empresa}, Fecha de pago: {fecha_pago}, Cuotas: {cuotas}")

        # Insertar los datos en la base de datos
        cursor.execute("""
            INSERT INTO pagos (dni, oponente, monto, empresa, fecha_pago, cuotas)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (dni_text, self.oponente, self.monto, self.empresa, fecha_pago, cuotas))

        logging.info(f"Pago insertado correctamente con DNI: {dni_text} y Monto: {self.monto}")
    except Exception as e:
        logging.error(f"Error al intentar guardar el pago con DNI: {self.dni} y Monto: {self.monto}. Error: {e}")
        raise  # Vuelve a lanzar la excepción después de loguearla





def check_duplicate_payment(dni, monto, empresa):
    """
    Verifica si hay pagos duplicados en la base de datos.
    :param dni: DNI del pago
    :param monto: Monto del pago
    :param empresa: Empresa asociada al pago
    :return: True si el pago ya existe, False si no
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    # Convertir `dni` a texto explícitamente
    cursor.execute("""
        SELECT 1 FROM pagos WHERE dni = %s AND monto = %s AND empresa = %s
    """, (str(dni), monto, empresa))  # Convertimos `dni` a string (TEXT)

    result = cursor.fetchone()
    cursor.close()
    connection.close()

    return result is not None


def check_valid_range(dni, monto):
    # Validar rango del dni (debe ser un número dentro del rango de INTEGER)
    if not (0 <= dni <= 2147483647):
        raise ValueError(f"DNI {dni} fuera del rango permitido para INTEGER.")
    
    # Validar rango del monto (debe estar dentro del rango de INTEGER)
    if not (-2147483648 <= monto <= 2147483647):
        raise ValueError(f"Monto {monto} fuera del rango permitido para INTEGER.")
    
    return True