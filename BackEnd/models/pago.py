class Pago:
    def __init__(self, dni, oponente, monto, empresa, fecha_pago=None, cuotas=None):
        self.dni = dni
        self.oponente = oponente
        self.monto = monto
        self.empresa = empresa
        self.fecha_pago = fecha_pago
        self.cuotas = cuotas

    def save_to_db(self, cursor):
        cursor.execute("""
            INSERT INTO pagos (dni, oponente, monto, empresa, fecha_pago, cuotas)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (self.dni, self.oponente, self.monto, self.empresa, self.fecha_pago, self.cuotas))

    def to_dict(self):
        # Devuelve un diccionario con los atributos de la instancia
        return {
            'dni': self.dni,
            'oponente': self.oponente,
            'monto': self.monto,
            'empresa': self.empresa,
            'fecha_pago': self.fecha_pago,
            'cuotas': self.cuotas
        }
