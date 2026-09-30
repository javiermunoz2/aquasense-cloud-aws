import json
import os
import boto3
import csv
from datetime import datetime
from decimal import Decimal

# ---- CONFIGURACION ----
# Se configuran como variables de entorno de la funcion Lambda
DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE', 'TemperatureData')
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']  # arn:aws:sns:<region>:<account-id>:proy-alertas

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
table = dynamodb.Table(DYNAMO_TABLE)


def parse_fecha(raw):
    """Acepta YYYY/MM/DD y DD/MM/YYYY, con o sin ceros, y con - o /."""
    raw = (raw or '').strip().replace('-', '/')
    parts = raw.split('/')
    if len(parts) != 3:
        return None
    try:
        if len(parts[0]) == 4:          # formato YYYY/MM/DD
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
        else:                            # formato DD/MM/YYYY
            d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
        return datetime(y, m, d)
    except (ValueError, TypeError):
        return None


def lambda_handler(event, context):
    try:
        # 1. Archivo que disparo el evento (trigger S3)
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print(f"Procesando archivo: {key} (bucket: {bucket})")

        # 2. Leer el CSV de entrada
        obj = s3.get_object(Bucket=bucket, Key=key)
        cuerpo = obj['Body'].read().decode('utf-8').splitlines()
        reader = csv.DictReader(cuerpo)

        # Semanas nuevas agrupadas por mes:
        nuevas = {}

        # 3. Recorrer filas: disparar alarmas y agrupar datos
        for row in reader:
            dt = parse_fecha(row.get('Fecha', ''))
            if dt is None:
                continue
            try:
                media = float(row['Medias'])
                desviacion = float(row['Desviaciones'])
            except (ValueError, KeyError, TypeError):
                continue

            # 3.1 Alarma por semana (requisito 1: desviacion > 0.5)
            if desviacion > 0.5:
                fecha_str = dt.strftime('%Y/%m/%d')
                mensaje = (f"ALARMA DE DESVIACION\n"
                           f"Fecha: {fecha_str}\n"
                           f"Desviacion detectada: {desviacion}")
                try:
                    sns.publish(TopicArn=SNS_TOPIC_ARN,
                                Message=mensaje,
                                Subject="Alerta: desviacion > 0.5")
                except Exception as e_sns:
                    print(f"Error enviando SNS: {e_sns}")

            # 3.2 Agrupar por mes y por dia
            clave_mes = dt.strftime('%Y/%m')
            clave_dia = dt.strftime('%Y/%m/%d')
            nuevas.setdefault(clave_mes, {})[clave_dia] = {
                'media': media,
                'desviacion': desviacion
            }

        # 4. Por cada mes afectado: leer lo existente, FUSIONAR y recalcular
        for mes, semanas_nuevas in nuevas.items():
            resp = table.get_item(Key={'Fecha': mes}, ConsistentRead=True)
            item = resp.get('Item')

            # Cargar semanas ya guardadas
            semanas = {}
            if item and 'Semanas' in item:
                for dia, vals in item['Semanas'].items():
                    semanas[dia] = {
                        'media': float(vals['media']),
                        'desviacion': float(vals['desviacion'])
                    }

            # Las nuevas sobrescriben: el ultimo fichero gana en fechas repetidas
            for dia, vals in semanas_nuevas.items():
                semanas[dia] = vals

            # Recalcular agregados del mes con TODAS sus semanas
            medias = [v['media'] for v in semanas.values()]
            desvs = [v['desviacion'] for v in semanas.values()]
            temp_media = sum(medias) / len(medias)
            max_temp = max(medias)
            max_desv = max(desvs)

            anio, num_mes = mes.split('/')
            semanas_dynamo = {
                dia: {'media': Decimal(str(v['media'])),
                      'desviacion': Decimal(str(v['desviacion']))}
                for dia, v in semanas.items()
            }

            table.put_item(Item={
                'Fecha': mes,
                'Year': int(anio),
                'Month': int(num_mes),
                'Semanas': semanas_dynamo,
                'TempMedia': Decimal(str(round(temp_media, 4))),
                'MaxTemp': Decimal(str(round(max_temp, 4))),
                'MaxDesviacion': Decimal(str(round(max_desv, 4))),
                'MaxDiff': Decimal('0')   # se recalcula en el paso 5
            })

        # 5. Recalcular MaxDiff de TODOS los meses
        todos = []
        resp = table.scan(ConsistentRead=True)
        todos.extend(resp.get('Items', []))
        while 'LastEvaluatedKey' in resp:
            resp = table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'],
                              ConsistentRead=True)
            todos.extend(resp.get('Items', []))

        meses_ordenados = sorted(todos, key=lambda it: it['Fecha'])
        prev_max = None
        for it in meses_ordenados:
            max_temp = float(it.get('MaxTemp', 0))
            diff = 0.0 if prev_max is None else (max_temp - prev_max)
            table.update_item(
                Key={'Fecha': it['Fecha']},
                UpdateExpression='SET MaxDiff = :d',
                ExpressionAttributeValues={':d': Decimal(str(round(diff, 4)))}
            )
            prev_max = max_temp

        print(f"Fichero: {len(nuevas)} meses. Total en tabla: {len(meses_ordenados)} meses.")
        return {'statusCode': 200, 'body': json.dumps('Proceso completado correctamente')}

    except Exception as e:
        print(f"ERROR CRITICO: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f"Error: {str(e)}")}