# BackEnd/models/pago.py
class Pago:
    def __init__(self, dni, oponente, monto, empresa, fecha_pago, cuotas, record_hash):
        self.dni = dni
        self.oponente = oponente
        self.monto = monto
        self.empresa = empresa
        self.fecha_pago = fecha_pago
        self.cuotas = cuotas
        self.record_hash = record_hash

    def to_tuple(self):
        return (
            self.dni,
            self.oponente,
            self.monto,
            self.empresa,
            self.fecha_pago,
            self.cuotas,
            self.record_hash
        )

    def to_dict(self):
        return {
            "dni": self.dni,
            "oponente": self.oponente,
            "monto": self.monto,
            "empresa": self.empresa,
            "fecha_pago": self.fecha_pago,
            "cuotas": self.cuotas,
            "record_hash": self.record_hash
        }
