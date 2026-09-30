from flask import Flask, request, jsonify
import boto3
from decimal import Decimal

app = Flask(__name__)

DYNAMO_TABLE = 'TemperatureData'
REGION = 'us-east-1'

dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(DYNAMO_TABLE)

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError
app.json.default = decimal_default

@app.route('/')
def root():
    return jsonify({'status': 'ok'}), 200

@app.route('/health')
def health():
    return jsonify({'status': 'healthy'}), 200

def get_data(year, month, attribute):
    if not year or not month:
        return None, "Faltan parametros"
    key_id = f"{year}/{str(month).zfill(2)}"
    try:
        response = table.get_item(Key={'Fecha': key_id})
        item = response.get('Item')
        if not item:
            return None, "Datos no encontrados"
        return item.get(attribute), None
    except Exception as e:
        return None, str(e)

@app.route('/temp')
def get_temp():
    y, m = request.args.get('year'), request.args.get('month')
    val, err = get_data(y, m, 'TempMedia')
    if err:
        return jsonify({'error': err}), 404
    return jsonify({'year': int(y), 'month': int(m), 'temp_media': val})

@app.route('/maxdiff')
def get_maxdiff():
    y, m = request.args.get('year'), request.args.get('month')
    val, err = get_data(y, m, 'MaxDiff')
    if err:
        return jsonify({'error': err}), 404
    return jsonify({'year': int(y), 'month': int(m), 'max_diff': val})

@app.route('/sd')
def get_sd():
    y, m = request.args.get('year'), request.args.get('month')
    val, err = get_data(y, m, 'MaxDesviacion')
    if err:
        return jsonify({'error': err}), 404
    return jsonify({'year': int(y), 'month': int(m), 'max_desviacion': val})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
