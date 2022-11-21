import fileinput
import numpy as np
import shutil
import pandas as pd


def createKML(templatePath, filePath, df):
    
    df.fillna(0, inplace=True)
    
    
    df['long_next'] = df['long'].shift(-1, fill_value=0)
    df['lat_next'] = df['lat'].shift(-1, fill_value=0)
    
    df['time_position']= pd.to_datetime(df['time_position'])
    df['time_position_next'] = df['time_position'].shift(-1)
    df['heading'] = np.arctan2(df['long_next']-df['long'], df['lat_next']-df['lat']) * 180 / np.pi
    df.to_csv('Inspect.csv')
    df = df.iloc[:-1 , :]
    
    shutil.copyfile(templatePath, filePath)
    
    template = "\t\t<gx:FlyTo>\n" + \
            "\t\t\t<gx:duration>{DUR}</gx:duration>\n" + \
            "\t\t\t<gx:flyToMode>smooth</gx:flyToMode>\n" + \
            "\t\t\t<LookAt>\n" + \
            "\t\t\t\t<longitude>{LONG}</longitude>\n" + \
            "\t\t\t\t<latitude>{LAT}</latitude>\n" + \
            "\t\t\t\t<altitude>{ALT}</altitude>\n" + \
            "\t\t\t\t<heading>{HEADING}</heading>\n" + \
            "\t\t\t\t<tilt>70</tilt>\n" + \
            "\t\t\t\t<range>100</range>\n" + \
            "\t\t\t\t<altitudeMode>relativeToGround</altitudeMode>\n" + \
            "\t\t\t</LookAt>\n" + \
            "\t\t</gx:FlyTo>\n"
    
    insert = ""
    
    
    for index, row in df.iterrows():
        # print(row['c1'], row['c2'])
        # heading_deg = math.atan2(lat_next-lat, long_next-long) * 180 / math.pi
        insert = insert + template.format(
            DUR=(row['time_position_next'] - row['time_position']).total_seconds()/60, 
            LONG=row['long'], 
            LAT=row['lat'], 
            ALT=row['baro_altitude'],
            HEADING=row['heading']
            )
    
    insert = insert.replace('nan', '0')
    
    find = "<gx:Playlist>"
    
    for line in fileinput.FileInput(filePath, inplace=True):
        if find in line:
            line += insert
        print(line, end="")
        
        
df = pd.read_csv('Test_data_2.csv')
df = df.sort_values(by='time_position')

createKML('Tour Template v2.kml', 'Test.kml', df)