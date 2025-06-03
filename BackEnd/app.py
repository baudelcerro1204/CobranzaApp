# app.py
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from services.pago_service import process_file
from services.report_service import generate_report, get_empresas, search_pagos, get_monthly_summary

app = Flask(__name__)
CORS(app)

# Endpoint para procesar archivo Excel y guardar los pagos
@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No se envió ningún archivo."}), 400

        file = request.files['file']

        if file.filename == '':
            return jsonify({"error": "Nombre de archivo vacío."}), 400

        # Procesar el archivo Excel y guardar los pagos en la base de datos
        pagos_no_duplicados = process_file(file)

        # Devolver la cantidad de pagos procesados y los datos serializados
        return jsonify({
            "message": f"{len(pagos_no_duplicados)} pagos procesados y guardados.",
            "pagos_no_duplicados": [pago.to_dict() for pago in pagos_no_duplicados]  # <== Esto resuelve tu error
        }), 200

    except Exception as e:
        # Loguear o mostrar el error
        print(f"Error procesando el archivo: {e}")
        return jsonify({"error": f"Error procesando el archivo: {str(e)}"}), 500

@app.route('/empresas', methods=['GET'])
def empresas():
    try:
        return jsonify(get_empresas())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/report', methods=['GET'])
def report():
    empresa      = request.args.get('empresa') or None
    fecha_inicio = request.args.get('fecha_inicio')
    fecha_fin    = request.args.get('fecha_fin')

    if not fecha_inicio or not fecha_fin:
        return jsonify({'error': 'Se requieren fecha_inicio y fecha_fin'}), 400

    try:
        data = generate_report(fecha_inicio, fecha_fin, empresa)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/search', methods=['GET'])
def search():
    empresa      = request.args.get('empresa') or None
    fecha_inicio = request.args.get('fecha_inicio')
    fecha_fin    = request.args.get('fecha_fin')

    if not fecha_inicio or not fecha_fin:
        return jsonify({'error': 'Se requieren fecha_inicio y fecha_fin'}), 400

    try:
        pagos = search_pagos(empresa, fecha_inicio, fecha_fin)
        return jsonify(pagos)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/report/monthly', methods=['GET'])
def report_monthly():
    empresa      = request.args.get('empresa') or None
    fecha_inicio = request.args.get('fecha_inicio')
    fecha_fin    = request.args.get('fecha_fin')
    if not fecha_inicio or not fecha_fin:
        return jsonify({'error':'Se requieren fecha_inicio y fecha_fin'}),400
    try:
        data = get_monthly_summary(fecha_inicio, fecha_fin, empresa)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error':str(e)}),500

if __name__ == '__main__':
    app.run(debug=True)
