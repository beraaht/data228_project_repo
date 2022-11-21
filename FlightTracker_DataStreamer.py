# The code below is based on the following tutorial:
# https://www.geodose.com/2020/08/create-flight-tracking-apps-using-python-open-data.html 

import requests
import json
import boto3
import time
from datetime import datetime
import smtplib
from email.message import EmailMessage

#AREA EXTENT COORDINATE WGS4
lon_min,lat_min = -125.974, 30.038
lon_max,lat_max = -68.748, 52.214

#REST API QUERY
user_name = ''
password = ''
url_data = 'https://'+user_name+':'+password \
        +'@opensky-network.org/api/states/all?' \
        +'lamin='+str(lat_min)+'&lomin='+str(lon_min) \
        +'&lamax='+str(lat_max)+'&lomax='+str(lon_max)
        
firehose_client = boto3.client('firehose', region_name='us-east-1',
     aws_access_key_id='',
     aws_secret_access_key=''
    )

email_address = ""
email_password = ""


iter = 0
while (iter < 1):
    print('Job #' + str(iter))
    now = str(datetime.now())
    print('GET (opensky-network) - Time: ' + now)
    

    response = requests.get(url_data).json()
    
    #LOAD TO PANDAS DATAFRAME
    col_name=['icao24','callsign','origin_country','time_position','last_contact','long','lat','baro_altitude','on_ground','velocity',       
    'true_track','vertical_rate','sensors','geo_altitude','squawk','spi','position_source']
    
    records = []
    for row in response['states']:
        
      # Convert epoch integers into datetime strings
      row[3] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row[3]))
      row[4] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row[4]))
      
      row_json = dict(zip(col_name, row))
      record = {"Data": json.dumps(row_json)}
      records.append(record)
    print('Received ' + str(len(records)) + ' data rows from GET')
    
    n = 0
    while True:
      print('PUT rows ' + str(n+1) + ' to ' + str(min(len(records), n + 50)))
      put_response = firehose_client.put_record_batch(DeliveryStreamName='data228-project-kinesis', Records=records[n : min(len(records), n + 500)])
      print(put_response)
      n = n + 500
      time.sleep(0.1)
    
    if iter % 20 == 0:
        msg = EmailMessage()
        msg['Subject'] = "FlightTracker Stream Report"
        msg['From'] = email_address
        msg['To'] = ""
        msg.set_content('Job #' + str(iter) + ' complete!')
    
        # send email
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(email_address, email_password)
            smtp.send_message(msg)

    iter = iter + 1
