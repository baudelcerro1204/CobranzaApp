# BackEnd/models/schemas.py

from pydantic import BaseModel, root_validator, field_validator
from typing import Optional
from datetime import date
import hashlib

class PagoSchema(BaseModel):
    dni: str
    monto: float
    empresa: str
    oponente: Optional[str] = None
    fecha_pago: Optional[date] = None
    cuotas: Optional[str] = None
    record_hash: str

    # Antes de cualquier validación, convertimos a str
    @field_validator('dni', mode='before')
    def convert_dni_to_str(cls, v):
        if v is None:
            return v
        return str(v).strip()

    # Ahora validamos que no quede vacío
    @field_validator('dni')
    def dni_not_empty(cls, v):
        if not v:
            raise ValueError('DNI vacío')
        return v

    @field_validator('monto')
    def monto_positive(cls, v):
        if v <= 0:
            raise ValueError('El monto debe ser mayor a cero')
        return round(v, 2)

    @field_validator('empresa')
    def empresa_not_empty(cls, v):
        v = v.strip()
        if not v:
            raise ValueError('Empresa vacío')
        return v

    @root_validator(pre=True)
    def compute_hash(cls, values):
        dni     = str(values.get('dni','')).strip()
        monto   = round(float(values.get('monto',0)), 2)
        empresa = str(values.get('empresa','')).strip()
        fecha   = values.get('fecha_pago') or ''
        cuotas  = values.get('cuotas') or ''
        raw = f"{dni}|{monto:.2f}|{empresa}|{fecha}|{cuotas}"
        values['record_hash'] = hashlib.sha256(raw.encode('utf-8')).hexdigest()
        return values
