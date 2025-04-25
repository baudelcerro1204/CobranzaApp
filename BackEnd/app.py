# app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
from services.pago_service import process_excel
from services.report_service import generate_report, get_empresas, search_pagos

app = Flask(__name__)

CORS(app)

# Endpoint para procesar archivo Excel y guardar los pagos
@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    
    # Procesar el archivo Excel y guardar los pagos en la base de datos
    pagos_no_duplicados = process_excel(file)

    return jsonify({
        "message": f"{len(pagos_no_duplicados)} pagos procesados y guardados.",
        "pagos_no_duplicados": pagos_no_duplicados
    }), 200

# Endpoint para obtener todas las empresas de la base de datos
@app.route('/empresas', methods=['GET'])
def empresas():
    empresas_list = get_empresas()
    return jsonify(empresas_list), 200

# Endpoint para generar el reporte de pagos
@app.route('/report', methods=['GET'])
def report():
    fecha_inicio = request.args.get('fecha_inicio')
    fecha_fin = request.args.get('fecha_fin')
    empresa = request.args.get('empresa')

    if not fecha_inicio or not fecha_fin:
        return jsonify({"error": "Se deben proporcionar las fechas de inicio y fin"}), 400

    report_data = generate_report(fecha_inicio, fecha_fin, empresa)

    return jsonify(report_data), 200

# Endpoint para buscar pagos por empresa y rango de fechas
@app.route('/pagos', methods=['GET'])
def pagos():
    empresa = request.args.get('empresa')
    fecha_inicio = request.args.get('fecha_inicio')
    fecha_fin = request.args.get('fecha_fin')

    if not fecha_inicio or not fecha_fin:
        return jsonify({"error": "Se deben proporcionar las fechas de inicio y fin"}), 400

    pagos_data = search_pagos(empresa, fecha_inicio, fecha_fin)
    return jsonify(pagos_data), 200

if __name__ == '__main__':
    app.run(debug=True)