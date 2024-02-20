#%% This code creates an SQL database for each qaqc wx station databases
# all databases are by default filled with nans. The code also assigns the primary
# column in each newly created SQL dabase to DateTime

####### Make sure of the following when you add SQL databases ####### 
# You need to make sure you export the variables with the correct data type. All
# '_flags' should be text or char. All others should be float, except for 
# Datetime and WatYr. If you don't do that, the qaqc_minapp won't work as it
# relies on the same data types across 'clean' and 'qaqc' databases. This can be
# done in python or directly in phpMyAdmin, using the following command:
#ALTER TABLE `qaqc_apelake` CHANGE `Air_Temp` `Air_Temp` FLOAT NULL DEFAULT NULL, 
#CHANGE `BP` `BP` FLOAT NULL DEFAULT NULL, CHANGE `Batt` `Batt` FLOAT NULL 
#DEFAULT NULL, CHANGE `LWL` `LWL` FLOAT NULL DEFAULT NULL, CHANGE `LWU` `LWU` 
#FLOAT NULL DEFAULT NULL, CHANGE `Lysimeter` `Lysimeter` FLOAT NULL DEFAULT 
#NULL, CHANGE `PC_Raw_Pipe` `PC_Raw_Pipe` FLOAT NULL DEFAULT NULL, CHANGE 
#`PC_Tipper` `PC_Tipper` FLOAT NULL DEFAULT NULL, CHANGE `PP_Pipe` `PP_Pipe` 
#FLOAT NULL DEFAULT NULL, CHANGE `PP_Tipper` `PP_Tipper` FLOAT NULL DEFAULT NULL, 
#CHANGE `Pk_Wind_Dir` `Pk_Wind_Dir` FLOAT NULL DEFAULT NULL, CHANGE `Pk_Wind_Speed`
# `Pk_Wind_Speed` FLOAT NULL DEFAULT NULL, CHANGE `RH` `RH` FLOAT NULL DEFAULT 
#NULL, CHANGE `SWE` `SWE` FLOAT NULL DEFAULT NULL, CHANGE `SWL` `SWL` FLOAT NULL 
#DEFAULT NULL, CHANGE `SWU` `SWU` FLOAT NULL DEFAULT NULL, CHANGE `Snow_Depth` 
#`Snow_Depth` FLOAT NULL DEFAULT NULL, CHANGE `Soil_Moisture` `Soil_Moisture` 
#FLOAT NULL DEFAULT NULL, CHANGE `Soil_Temperature` `Soil_Temperature` FLOAT 
#NULL DEFAULT NULL, CHANGE `Solar_Rad` `Solar_Rad` FLOAT NULL DEFAULT NULL, 
#CHANGE `Wind_Dir` `Wind_Dir` FLOAT NULL DEFAULT NULL, CHANGE `Wind_Speed` 
#`Wind_Speed` FLOAT NULL DEFAULT NULL;

# Written by Julien Bodart (VIU) - 12/13/2023

import pandas as pd 
from sqlalchemy import create_engine
import numpy as np
import re
import os
from datetime import datetime

#%% import support functions
os.chdir('D:/GitHub/QAQC_wx_data')
import qaqc_functions

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
wx_stations = [x for x in wx_stations if not "_test" in x] # remove legacy data for Cairnridgerun
wx_stations = [x for x in wx_stations if not "clean_eastbuxton_archive" in x] # remove machmell from list
wx_stations = [x for x in wx_stations if not "clean_steph10" in x] # remove legacy data for Cairnridgerun\wx_stations = [w.replace('clean_eastbuxton_archive', 'clean_temp') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [w.replace('clean_machmellkliniklini', 'clean_Machmellkliniklini') for w in wx_stations] # rename machmellkliniklini so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "russell" in x] # remove russell from list
wx_stations = [w.replace('clean_Machmellkliniklini', 'clean_machmellkliniklini') for w in wx_stations] # rename machmellkliniklini back to original

# deal with Stephanies
keep_steph = False

# if you don't want Stephs, then remove all except from Steph 3 and 6 which are satellite connected
if keep_steph == False:
    wx_stations = [w.replace('clean_steph3', 'clean_Stephanie3') for w in wx_stations] # rename steph3 so it doesn't get cut out
    wx_stations = [w.replace('clean_steph6', 'clean_Stephanie6') for w in wx_stations] # rename steph6 so it doesn't get cut out
    wx_stations = [x for x in wx_stations if not "steph" in x] # remove all stephanies
    wx_stations = [w.replace('clean_Stephanie3', 'clean_steph3') for w in wx_stations] # rename steph3 back to original
    wx_stations = [w.replace('clean_Stephanie6', 'clean_steph6') for w in wx_stations] # rename steph6 back to original
    stephs = []
    
# else if you want Stephanies, select specific ones 
else:
    stephs = ['steph1', 'steph2', 'steph4', 'steph6', 'steph7', 'steph8'] # string with all Stephanies except Steph3

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
        
    #%% Only select earliest possible date for full year
    dt_sql = pd.to_datetime(sql_file['DateTime'])    
    yr_str = dt_sql[0].year # index of year 1
    dt_str = np.flatnonzero(dt_sql >= np.datetime64(datetime(yr_str, 10, 1, 00, 00, 00)))[0] # index of full water year for start of timeseries
    
    #%% only keep data from oldest to newest default date except for exceptions  
    # Stephs not connected to satellite have data up to Oct 2023
    if wx_stations_name[l] in stephs: 
        end_yr_sql = qaqc_functions.nearest(dt_sql, datetime(2023, 9, 30, 23, 00, 00))
        new_df = sql_file.loc[:int(np.flatnonzero(sql_file['DateTime'] == end_yr_sql))]
        
    # Mt Maya went offline in Nov 2024
    elif wx_stations_name[l] == 'mountmaya':
        end_yr_sql = qaqc_functions.nearest(dt_sql, datetime(2024, 1, 11, 7, 0, 0))
        new_df = sql_file.loc[:int(np.flatnonzero(sql_file['DateTime'] == end_yr_sql))]

    # Machmell went offline in Feb 2023
    elif wx_stations_name[l] == 'machmell':
        end_yr_sql = qaqc_functions.nearest(dt_sql, datetime(2023, 2, 12, 11, 00, 00))
        new_df = sql_file.loc[:int(np.flatnonzero(sql_file['DateTime'] == end_yr_sql))]
        
    # otherwise if any other stations, then select Feb 2024 as latest date
    else:
        new_df = sql_file[:int(np.flatnonzero(sql_file['DateTime'] == '2024-02-19 07:00:00'))]
    
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
