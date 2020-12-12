from datetime import datetime
import time
import base64
import json
import sqlalchemy
import pytz
connection_name = ""
db_name = ""
db_user = ""
db_password = ""
driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})
def hello_pubsub(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    x = json.loads(pubsub_message)
    deviceid = x['device_id']
    roomid = x['roomid']
    dt_object = datetime.fromtimestamp(x['Date'], pytz.timezone('Asia/Bangkok'))
    time_string = str(dt_object.strftime("%Y-%m-%dT%H:%M:%SZ"))
    temp = x['Temp']
    humi = x['Humi']
    sql = "INSERT INTO dht11(device_id, room_id, datetime, temperature, humidity) VALUES (%s,%s,%s,%s,%s)"
    db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
        drivername=driver_name,
        username=db_user,
        password=db_password,
        database=db_name,
        query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )
    try:
        with db.connect() as conn:
            conn.execute(sql, (deviceid, roomid, time_string, temp, humi))
    except Exception as e:
        return 'Error: {}'.format(str(e))
    print(type(dt_object))
