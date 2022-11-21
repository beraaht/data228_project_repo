'''
The Python script below is based on the following tutorial:
https://www.geodose.com/2020/08/create-flight-tracking-apps-using-python-open-data.html
FLIGHT TRACKING WITH PYTHON AND OPEN AIR TRAFFIC DATA
by ideagora geomatics | www.geodose.com | @ideageo
'''
#IMPORT LIBRARY
import pandas as pd
from bokeh.plotting import figure, gmap
from bokeh.models import HoverTool,Label,LabelSet,ColumnDataSource, GMapOptions,Button,Slider,TextAreaInput
from bokeh.layouts import column,row,layout
from bokeh.tile_providers import get_provider, STAMEN_TERRAIN
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from AthenaToDataframe import QueryAthena
from WriteKML import createKML

gmaps_api_key = ''

flight_df = pd.DataFrame()

def queryAthena(beginning, end, icao24):
    global flight_df
    
    query = 'select * from "data228-project-table" where icao24=\'' + icao24 + '\'' \
                + ' and time_position >= parse_datetime(\'' + beginning \
                + '\', \'yyyy-MM-dd HH:mm:ss\')' \
                + ' and time_position <= parse_datetime(\'' + end \
                + '\', \'yyyy-MM-dd HH:mm:ss\')' 
    qa = QueryAthena(query=query, database='data228-project-database')
    flight_df = qa.run_query()
    flight_df=flight_df.iloc[:,0:17]

    col_name=['icao24','callsign','origin_country','time_position','last_contact','long','lat','baro_altitude','on_ground','velocity',       
    'true_track','vertical_rate','sensors','geo_altitude','squawk','spi','position_source']
    flight_df.columns=col_name
    flight_df=flight_df.fillna('No Data')
    flight_df['rot_angle']=flight_df['true_track']*-1
    icon_url='https://findicons.com/files/icons/770/token_dark/128/airplane.png' #icon url
    flight_df['url']=icon_url
    flight_df = flight_df.sort_values(by=['time_position'])
    
 
#FLIGHT TRACKING FUNCTION
def flight_tracking(doc):
    # init bokeh column data source
    flight_source = ColumnDataSource({
        'icao24':[],'callsign':[],'origin_country':[],
        'time_position':[],'last_contact':[],'long':[],'lat':[],
        'baro_altitude':[],'on_ground':[],'velocity':[],'true_track':[],
        'vertical_rate':[],'sensors':[],'geo_altitude':[],'squawk':[],'spi':[],'position_source':[],
        'rot_angle':[],'url':[]
    })
    
    # UPDATING FLIGHT DATA
    n = 0
    n_max = len(flight_df.index)
    
    def update():
        nonlocal n, n_max
        n_max = len(flight_df.index)
        
        # CONVERT TO BOKEH DATASOURCE AND STREAMING
        flight_df_single = flight_df.iloc[[n]]
        flight_source.stream(flight_df_single.to_dict(orient='list'),1)
        if n < n_max - 1:
            n = n + 1
        else:
            n = 0
        print('Running...' + str(flight_df_single.iloc[0]['long']) + ' & ' + str(flight_df_single.iloc[0]['lat']))
        
    #CALLBACK UPATE IN AN INTERVAL
    callback_id = None
    
    #doc.add_periodic_callback(update, 1000) #5000 ms/10000 ms for registered user ..     
    #PLOT AIRCRAFT POSITION  
    map_options = GMapOptions(lat=40, lng=-100, map_type="roadmap", zoom=4)
    p = gmap(gmaps_api_key, map_options, title="Flight Tracker v1.0", width=1024, height=512)
    
    p.image_url(url='url', x='long', y='lat',source=flight_source,anchor='center',angle_units='deg',angle='rot_angle',h_units='screen',w_units='screen',w=40,h=40)

    labels = LabelSet(x='long', y='lat', text='time_position', level='glyph',
            x_offset=10, y_offset=10, source=flight_source, render_mode='canvas',
            border_line_color='black', border_line_alpha=1.0, text_color='white',
            background_fill_color='#1e7898',text_font_size="10pt")
    p.add_layout(labels)
    
    interval = 1000
    
    def slider_interval_action(attr, old, new):
        nonlocal interval
        interval = round(slider_interval.value * 1000, 0)
        print("interval: " + str(interval))
    
    slider_interval = Slider(start=0.1, end=5, value=1,
                step=0.1, title='Interval')
    slider_interval.on_change('value', slider_interval_action)
    
    
    def btn_start_action():
        nonlocal callback_id, n
        if callback_id == None:
            n = 0
            callback_id = doc.add_periodic_callback(update, interval)
        
    def btn_stop_action():
        nonlocal callback_id
        doc.remove_periodic_callback(callback_id)
        callback_id = None
    
    
    btn_start = Button(label = "Start Animation")
    btn_start.on_click(btn_start_action)
    btn_stop = Button(label = "Stop Animation")
    btn_stop.on_click(btn_stop_action)
    
    
    def btn_updateData_action():
        global flight_df
        nonlocal n
        n = 0
        params = input_queryParams.value.split("\n")
        queryAthena(params[1], params[2], params[0])
        print('Dataframe updated.')
    
    def btn_createKML_action():
        df = flight_df.copy()
        tourFile = input_queryParams.value
        tourFile = 'Tour_' + tourFile.replace('\n', '_',).replace(':', '-') + '.kml'
        createKML('Tour Template v2.kml', tourFile, df)
    
    btn_updateData = Button(label = "Update Data")
    btn_updateData.on_click(btn_updateData_action)
    btn_createKML = Button(label = "Create Google Earth Tour")
    btn_createKML.on_click(btn_createKML_action)
    
    input_queryParams = TextAreaInput(value="ad285f\n2022-10-29 00:00:00\n2022-10-30 00:00:00",rows=3,title="Query parameters:")
    
    
    doc.title='REAL TIME FLIGHT TRACKING'
    doc.add_root(input_queryParams)
    doc.add_root(row(btn_updateData, btn_createKML))
    doc.add_root(row(slider_interval, btn_start, btn_stop))
    doc.add_root(p)

# SERVER CODE
apps = {'/': Application(FunctionHandler(flight_tracking))}
server = Server(apps, port=8084) #define an unused port
server.start()
