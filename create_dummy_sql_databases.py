#%% This code creates an SQL database for each qaqc wx station databases
# all databases are by default filled with nans. The code also assigns the primary
# column in each newly created SQL dabase to DateTime

# Written by Julien Bodart (VIU) - 12/13/2023

import pandas as pd 
from sqlalchemy import create_engine
import numpy as np
import re
import copy

#%% Establish a connection with MySQL database 'viuhydro_wx_data_v2'
engine = create_engine('mysql+mysqlconnector://viuhydro_shiny:.rt_BKD_SB*Q@192.99.62.147:3306/viuhydro_wx_data_v2', echo = False)

#%% extract name of all tables within SQL database and clean up var name list
connection = engine.raw_connection()
cursor = connection.cursor()
cursor.execute("Show tables;")
wx_stations_lst = cursor.fetchall()
wx_stations = []
for i in range(len(wx_stations_lst)):
     lst = (re.sub(r'[^\w\s]', '', str(wx_stations_lst[i])))
     wx_stations.append(lst)
   
# remove 'raw' tables, remove all steph (but steph3), and others due to local issues
# or because there is no snowDepth sensor there, and sort out the name formatting
wx_stations = [x for x in wx_stations if "clean" in x ]
wx_stations = [x for x in wx_stations if not "legacy_ontree" in x] # remove legacy data for Cairnridgerun
wx_stations = [w.replace('clean_steph3', 'clean_Stephanie3') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [w.replace('clean_eastbuxton_archive', 'clean_temp') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "clean_eastbuxton" in x] # remove machmell from list
wx_stations = [w.replace('clean_temp', 'clean_eastbuxton') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "steph" in x] # remove all stephanies
wx_stations = [w.replace('clean_machmellkliniklini', 'clean_Machmellkliniklini') for w in wx_stations] # rename machmellkliniklini so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "machmell" in x] # remove machmell from list
wx_stations = [x for x in wx_stations if not "russell" in x] # remove russell from list
wx_stations = [x for x in wx_stations if not "plummerhut" in x] # remove plummer from list
wx_stations = [w.replace('clean_Stephanie3', 'clean_steph3') for w in wx_stations] # rename steph3 back to original
wx_stations = [w.replace('clean_Machmellkliniklini', 'clean_machmellkliniklini') for w in wx_stations] # rename machmellkliniklini back to original
wx_stations_name = list(map(lambda st: str.replace(st, 'clean_', ''), wx_stations)) # remove 'clean_' for csv export
wx_stations_name_cap = [wx_name.capitalize() for wx_name in wx_stations_name] # capitalise station name

#%% Loop over each station at a time and clean up the snow depth variable
for l in range(len(wx_stations_name)):
    sql_database = wx_stations_name[l]
    sql_name = wx_stations_name_cap[l]
    print('###### Creating dummy sql database for station: %s ######' %(sql_name))     
    
    #%% import current data on SQL database and clean name of some columns to match
    # CSV column names
    sql_file = pd.read_sql(sql="SELECT * FROM clean_" + sql_database, con = engine)
    sql_file = sql_file.set_index('DateTime').asfreq('1H').reset_index() # make sure records are continuous every hour

    # Fix issue with Datlamen and Rennell where time is not rounded to nearest
    # hour which affects how the code below works
    if wx_stations_name[l] == 'datlamen' or 'rennellpass':
        df_dt = sql_file.set_index('DateTime') 
        dt_sql = copy.deepcopy(sql_file['DateTime'])
        dt_sql = pd.to_datetime(dt_sql)
        for i in range(len(sql_file)):
            dt_sql[i] = dt_sql[i].floor('H') # floor to nearest hour
            
        sql_file['DateTime'] = dt_sql
        
    #%% Only select earliest possible date for full year
    #dt_sql = pd.to_datetime(sql_file['DateTime'])    
    #yr_last = int(np.flatnonzero(sql_file['DateTime'] == '2023-10-01 00:00:00'))
    #yr_str = dt_sql[0].year # index of year 1
    #dt_str = np.flatnonzero(dt_sql >= np.datetime64(datetime(yr_str, 10, 1, 00, 00, 00)))[0] # index of full water year for start of timeseries
    
    #%% only keep data from oldest to last water year
    new_df = sql_file[:int(np.flatnonzero(sql_file['DateTime'] == '2023-10-01 00:00:00'))]
    #new_df = sql_file[dt_str:int(np.flatnonzero(sql_file['DateTime'] == '2023-10-01 00:00:00'))]
    
    nanout = [c for c in new_df.columns if c not in ['DateTime', 'WatYr']]
    new_df[nanout] = np.nan
    
    # add flags columns
    colname = nanout
    colname_flags = [direction + '_flags' for direction in colname]
    
    # merge both dataframes together
    new_df[colname_flags] = np.nan
    
    # sort columns by alphabetical names
    colname_new = new_df.columns[2:]
    temp_df = new_df[colname_new]
    temp_df = temp_df[sorted(temp_df.columns)]
    
    # merge into final dataframe
    df_full = pd.concat([new_df['DateTime'], new_df['WatYr'],temp_df],axis=1)
    
    # import database to SQL
    df_full.to_sql(name='qaqc_%s' %wx_stations_name[l], con=engine, if_exists = 'fail', index=False)
    
#%% make sure you assign 'DateTime' column as the primary column
for l in range(len(wx_stations_name)):
    print('## Assigning DateTime column as the primary column for station: %s ##' %(wx_stations_name[l]))     
    engine = create_engine('mysql+mysqlconnector://viuhydro_shiny:.rt_BKD_SB*Q@192.99.62.147:3306/viuhydro_wx_data_v2', echo = False)
    with engine.connect() as con:
        con.execute('ALTER TABLE `qaqc_%s`' %wx_stations_name[l] + ' ADD PRIMARY KEY (`DateTime`);')

#%%