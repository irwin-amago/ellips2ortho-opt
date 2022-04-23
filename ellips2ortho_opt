import pandas as pd
import requests
import streamlit as st
import pydeck as pdk
import zipfile
import asyncio
import nest_asyncio
from concurrent.futures import ThreadPoolExecutor

# Define functions for asynchronous processing

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
nest_asyncio.apply()

def request_height(session, i, lat, lon, geoid):
    
    print(lat,lon)
    
    cmd_h = 'https://geodesy.noaa.gov/api/ncat/llh?'    
    req_h = cmd_h + 'lat=' + str(lat) + '&lon=' + str(lon) + '&inDatum=nad83(1986)&outDatum=nad83(2011)'
    
    with session.get(req_h) as response:  
        lat_nad = response.json()['destLat']
        lon_nad = response.json()['destLon']
        if response.status_code != 200:
            print("FAILURE::{0}".format(req_h))
    
    print(lat_nad,lon_nad)

    cmd_v = 'https://geodesy.noaa.gov/api/geoid/ght?'    
    req_v = cmd_v + 'lat=' + lat_nad + '&lon=' + lon_nad + '&model=' + str(geoid)
  
    with session.get(req_v) as response:  
        geoid_h = response.json()['geoidHeight']
        if response.status_code != 200:
            print("FAILURE::{0}".format(req_v))
        return geoid_h

async def start_async_process(lat, lon, geoid):
    heights = []
    size = len(lat)
    with ThreadPoolExecutor(max_workers=size) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(
                    executor,
                    request_height,
                    *(session, i, lat[i], lon[i], geoid)
                )
                for i in range(size)
            ]
            for response in await asyncio.gather(*tasks):
                heights.append(response)
    return heights

# App definition and processing script

if __name__ == "__main__":
    st.set_page_config(layout="wide")
    
    st.title('Ellipsoidal to Orthometric Heights (US)')
    
    st.sidebar.image('./logo.png', width = 260)
    st.sidebar.markdown('#')
    st.sidebar.write('The application uses the NGS Geoid API to look up the geoid height at a particular location and uses this value to then compute the orthometric height based on the desired units of the user.')
    st.sidebar.write('To select the correct geoid model for your application, please visit: https://geodesy.noaa.gov/GEOID/.')
    st.sidebar.write('If you have any questions regarding the application, please contact us at support@wingtra.com.')
    st.sidebar.markdown('#')
    st.sidebar.info('This is a prototype application. Wingtra AG does not guarantee correct functionality. Use with discretion.')
    
    # Upload button for CSVs
    
    uploaded_csvs = st.file_uploader('Please Select Geotags CSV.', accept_multiple_files=True)
    uploaded = False
    
    for uploaded_csv in uploaded_csvs: 
        if uploaded_csv is not None:
            uploaded = True
        else:
            uplaoded = False
    
    # Checking if upload of all CSVs is successful
    
    required_columns = ['# image name',
                        'latitude [decimal degrees]',
                        'longitude [decimal degrees]',
                        'altitude [meter]',
                        'accuracy horizontal [meter]',
                        'accuracy vertical [meter]']
    
    if uploaded:
        dfs = []
        filenames = []
        df_dict = {}
        ctr = 0
        for uploaded_csv in uploaded_csvs:
            df = pd.read_csv(uploaded_csv, index_col=False)       
            dfs.append(df)
            df_dict[uploaded_csv.name] = ctr
            filenames.append(uploaded_csv.name)
            
            lat = 'latitude [decimal degrees]'
            lon = 'longitude [decimal degrees]'
            height = 'altitude [meter]'
            
            ctr += 1
            
            # Check if locations are within the United States
            
            url = 'http://api.geonames.org/countryCode?lat='
            geo_request = url + str(df[lat][0]) + '&lng=' + str(df[lon][0]) + '&type=json&username=irwinamago'
            country = requests.get(geo_request).json()['countryName']
            
            if country != 'United States':
                msg = 'Locations in ' + uploaded_csv.name + ' are outside the United States. Please remove to proceed.'
                st.error(msg)
                st.stop()
    
            # Check if CSV is in the correct format
            
            format_check = True
            for column in required_columns:
                if column not in list(df.columns):
                    st.text(column + ' is not in ' + uploaded_csv.name + '.')
                    format_check = False
            
            if not format_check:
                msg = uploaded_csv.name + ' is not in the correct format. Delete or reupload to proceed.'
                st.error(msg)
                st.stop()
        
        st.success('All CSVs checked and uploaded successfully.')
        
        map_options = filenames.copy()
        map_options.insert(0, '<select>')
        option = st.selectbox('Select geotags CSV to visualize', map_options)
        
        # Option to visualize any of the CSVs
        
        if option != '<select>':
            points_df = pd.concat([dfs[df_dict[option]][lat], dfs[df_dict[option]][lon]], axis=1, keys=['lat','lon'])
            
            st.pydeck_chart(pdk.Deck(
            map_style='mapbox://styles/mapbox/satellite-streets-v11',
            initial_view_state=pdk.ViewState(
                latitude=points_df['lat'].mean(),
                longitude=points_df['lon'].mean(),
                zoom=14,
                pitch=0,
             ),
             layers=[
                 pdk.Layer(
                     'ScatterplotLayer',
                     data=points_df,
                     get_position='[lon, lat]',
                     get_color='[70, 130, 180, 200]',
                     get_radius=20,
                 ),
                 ],
             ))
    
        geoid_dict = {'GEOID99': 1,
                      'G99SSS': 2,
                      'GEOID03': 3,
                      'USGG2003': 4,
                      'GEOID06': 5,
                      'USGG2009': 6,
                      'GEOID09': 7,
                      'The latest experimental Geoid (XUSHG)': 9,
                      'USGG2012': 11,
                      'GEOID12A': 12,
                      'GEOID12B': 13,
                      'GEOID18': 14}
        units_dict ={'Meters': 1, 'US Feet': 2}
        
        # Select Geoid Model
        
        geoid_options = list(geoid_dict.keys()).copy()
        geoid_options.insert(0, '<select>')
        geoid_select = st.selectbox('Please Choose Desired Geoid', geoid_options)
        
        if not geoid_select=='<select>':
            st.write('You selected:', geoid_select)
            geoid = geoid_dict[geoid_select]
        
        # Select Units for Conversion
        
        units_select = st.selectbox('Please Select Desired Units', ('<select>', 'Meters','US Feet'))
        
        if not units_select=='<select>':
            st.write('You selected:', units_select)
            units = units_dict[units_select]
        
        # Run Conversion
        
        if uploaded and not geoid_select=='<select>' and not units_select=='<select>':
            if st.button('CONVERT HEIGHTS'):
                file_ctr = 0
                for df in dfs:
                    st.text('Processing ' + filenames[file_ctr] + '.')
                    my_bar = st.progress(0)
                    
                    ortho = []
                    prog = 0
                    
                    x = 0
                    wdw = 15                    
                    while x < len(df[lat]):

                        lat_req = list(df[lat][x:x+wdw])
                        lon_req = list(df[lon][x:x+wdw])
                        
                        # Multithread Handling
                        
                        loop = asyncio.get_event_loop()        
                        future = asyncio.ensure_future(start_async_process(lat_req, lon_req, geoid))   
                        undulations = loop.run_until_complete(future)
                        
                        # Convert ellipsoidal heights within window to orthometric
                        
                        for y in range(len(undulations)):                           
                            ortho_height = float(df[height][x+y]) - undulations[y] 
                            
                            if units==1:
                                ortho.append(ortho_height)
                            else:
                                ortho.append(ortho_height*3.2808399)
                            
                            prog += 1
                            my_bar.progress(prog/len(df[lat]))

                        # Adjust the window
                        
                        if len(df[lat]) - x < wdw:
                            wdw2 = len(df[lat]) - x
                            wdw = wdw2
                        if wdw == 0:
                            break
                        
                        x += wdw
            
                    df[height] = ortho
                    file_ctr += 1
                    
                    if units==1:
                        df.rename(columns={height: 'orthometric height [meter]'}, inplace=True)
                    else:
                        df['accuracy horizontal [meter]'] = df['accuracy horizontal [meter]'].apply(lambda x: x*3.2808399)
                        df['accuracy vertical [meter]'] = df['accuracy vertical [meter]'].apply(lambda x: x*3.2808399)
                        df.rename(columns={height: 'orthometric height [feet]',
                                           'accuracy horizontal [meter]': 'accuracy horizontal [feet]', 
                                           'accuracy vertical [meter]': 'accuracy vertical [feet]'}, inplace=True)
                
                st.success('Height conversion finished. Click button below to download converted files.')
                
                
                # Create the zip file, convert the dataframes to CSV, and save inside the zip
                
                if len(dfs)==1:
                    csv = dfs[0].to_csv(index=False).encode('utf-8')
                    filename = filenames[0].split('.')[0] + '_orthometric.csv'
    
                    st.download_button(
                         label="Download Converted Geotags CSV",
                         data=csv,
                         file_name=filename,
                         mime='text/csv',
                     )
                    
                else:                
                    with zipfile.ZipFile('Converted_CSV.zip', 'w') as csv_zip:
                        file_ctr = 0
                        for df in dfs:
                            csv_zip.writestr(filenames[file_ctr].split('.')[0] + '_orthometric.csv', df.to_csv(index=False).encode('utf-8'))
                            file_ctr += 1   
                    
                    # Download button for the zip file
                    fp = open('Converted_CSV.zip', 'rb')
                    st.download_button(
                        label="Download Converted Geotags CSV",
                        data=fp,
                        file_name='Converted_CSV.zip',
                        mime='application/zip',
                )
        st.stop()
    else:
        st.stop()
