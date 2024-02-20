# This code attempts to QA/QC the RH data in a full year for all 
# wx stations and all years

# Written and modified by Julien Bodart (VIU) - 19/02/2024
import os
import pandas as pd 
import os.path
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import re
from pathlib import Path

#%% import support functions
os.chdir('D:/GitHub/QAQC_wx_data')
import qaqc_functions

# remove chained assignmnent warning from Python - be careful!
pd.set_option('mode.chained_assignment', None)

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
# or because there is no sensor there, and sort out the name formatting
wx_stations = [x for x in wx_stations if "clean" in x ]
wx_stations = [x for x in wx_stations if not "clean_eastbuxton_archive" in x] # remove machmell from list
wx_stations = [x for x in wx_stations if not "legacy_ontree" in x] # remove legacy data for Cairnridgerun
wx_stations = [w.replace('clean_steph3', 'clean_Stephanie3') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [w.replace('clean_steph6', 'clean_Stephanie6') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "steph" in x] # remove all stephanies
wx_stations = [x for x in wx_stations if not "russell" in x] # remove rennell from list
wx_stations = [w.replace('clean_Stephanie3', 'clean_steph3') for w in wx_stations] # rename steph3 back to original
wx_stations = [w.replace('clean_Stephanie6', 'clean_steph6') for w in wx_stations] # rename steph6 back to original
wx_stations_name = list(map(lambda st: str.replace(st, 'clean_', ''), wx_stations)) # remove 'clean_' for csv export
wx_stations_name_cap = [wx_name.capitalize() for wx_name in wx_stations_name] # capitalise station name

#%% Loop over each station at a time and clean up the RH variable
for l in range(len(wx_stations_name)):
    sql_database = wx_stations_name[l]
    sql_name = wx_stations_name_cap[l]
    
    print('###### Cleaning RH data for station: %s ######' %(sql_name))     
    
    # create new directory on Windows (if does not exist) and cd into it
    Path("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/v2/individual_figures/" + sql_database + "/RH").mkdir(parents=True, exist_ok=True)
    os.chdir("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/v2/individual_figures/" + sql_database + "/RH")
    
    #%% import current data on SQL database and clean name of some columns to match
    # CSV column names
    sql_file = pd.read_sql(sql="SELECT * FROM clean_" + sql_database, con = engine)
    #sql_file = sql_file.iloc[int(np.flatnonzero(sql_file['DateTime'] == '2015-10-01 00:00:00')):].reset_index(drop=True)
    qaqc_file_airTemp = pd.read_sql(sql="SELECT * FROM qaqc_" + sql_database, con = engine) # get air temp from already qaqced data
    #qaqc_file_airTemp = qaqc_file_airTemp.iloc[int(np.flatnonzero(qaqc_file_airTemp['DateTime'] == '2015-10-01 00:00:00')):].reset_index(drop=True)
    
    # make sure you only go as far as specific date for all wx stations for current water year
    # Mt Maya went offline in Nov 2024
    if wx_stations_name[l] == 'mountmaya':
        sql_file_idx_latest = int(np.flatnonzero(sql_file['DateTime'] == '2024-01-11 07:00:00')+1) # arbitrary date
        sql_file = sql_file[:sql_file_idx_latest]
    # Machmell went offline in Feb 2023
    elif wx_stations_name[l] == 'machmell':
        sql_file_idx_latest = int(np.flatnonzero(sql_file['DateTime'] == '2023-02-12 11:00:00')+1) # arbitrary date
        sql_file = sql_file[:sql_file_idx_latest]
    else:
        sql_file_idx_latest = int(np.flatnonzero(sql_file['DateTime'] == '2024-02-19 06:00:00')+1) # arbitrary date
        sql_file = sql_file[:sql_file_idx_latest]
    
    #%% Make sure there is no gap in datetime (all dates are consecutive) and place
    # nans in all other values if any gaps are identified
    df_dt = pd.Series.to_frame(sql_file['DateTime'])    
    sql_file = sql_file.set_index('DateTime').asfreq('1H').reset_index()
    dt_sql = pd.to_datetime(sql_file['DateTime'])
    
    #%% select variable you want to QA/QC
    var = 'RH'
    var_name = 'Relative Humidity'
    var_name_short = 'RH'
     
    #%% Loop over all years of weather station data and apply a QA/QC routine
    # get year range of dataset and loop through each year if these contain a full
    # 12-month water year
    if 10 <= datetime.now().month and datetime.now().month <= 12:
        yr_range = np.arange(dt_sql[0].year, datetime.now().year+1) # find min and max years
    elif wx_stations_name[l] == 'machmell': 
        yr_range = np.arange(dt_sql[0].year, datetime.now().year-1) # find min and max years
    else: 
        yr_range = np.arange(dt_sql[0].year, datetime.now().year) # find min and max years

    if wx_stations_name[l] == 'machmell':
       yr_range = np.delete(yr_range, np.flatnonzero(yr_range == 2022))
                
    qaqc_arr_final = []
    for k in range(len(yr_range)):
        print('## Cleaning data for year: %d-%d ##' %(yr_range[k],yr_range[k]+1)) 
    
        # find indices of water years
        start_yr_sql = qaqc_functions.nearest(dt_sql, datetime(yr_range[k], 10, 1))
        end_yr_sql = qaqc_functions.nearest(dt_sql, datetime(yr_range[k]+1, 9, 30, 23, 00, 00))
    
        # select data for the whole water year based on datetime object
        dt_yr = np.concatenate(([np.where(dt_sql == start_yr_sql), np.where(dt_sql == end_yr_sql)]))
    
        # store for plotting
        raw = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        qaqc_arr = sql_file.copy() # array to QAQC
        
        #%% Remove values above 100 or below 0% threshold
        # above 100%
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 2
        threshold = 100 # in %
        qaqc_2, flags_2 = qaqc_functions.reset_max_threshold(qaqc_arr[var], data, flag, threshold)
        qaqc_arr[var] = qaqc_2
        
        # below 5%
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 2
        threshold = 5 # in %
        qaqc_2, flags_2 = qaqc_functions.reset_min_threshold(qaqc_arr[var], data, flag, threshold)
        qaqc_arr[var] = qaqc_2
    
        #%% Apply static range test (remove values where difference is > than value)
        # Maximum value between each step: 85%
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 1
        step_size = 85 # in %
        qaqc_1, flags_1 = qaqc_functions.static_range_test(qaqc_arr[var], data, flag, step_size)
        qaqc_arr[var] = qaqc_1
                
        #%% Remove duplicate consecutive values == 100% or == 0% for RH
        # equal to 100%
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 3
        window = 120 # in hours (equivalent to 5 days)
        threshold = 100
        qaqc_3, flags_3 = qaqc_functions.duplicates_window(qaqc_arr[var], data, flag, window, threshold)
        qaqc_arr[var] = qaqc_3
        
        # equal to 0%        
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 3
        window = 2 # in hours
        threshold = 0
        qaqc_3, flags_3 = qaqc_functions.duplicates_window(qaqc_arr[var], data, flag, window, threshold)
        qaqc_arr[var] = qaqc_3
        
        #%% Remove non-sensical zero values if they are not bounded by a 
        # specific threshold for i-1 and i+1 (e.g. -3 to 3). This removes
        # false zeros in the data
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 6
        false_zero_threshold = 75 # in %
        qaqc_6, flags_6 = qaqc_functions.false_zero_removal(qaqc_arr[var], data, flag, false_zero_threshold)
        qaqc_arr[var] = qaqc_6
        
        #%% Interpolate nans with method='linear' using pandas.DataFrame.interpolate
        # First, identify gaps larger than 3 hours (which should not be interpolated).
        # Then, need to convert RH to EA before interpolating. This requires a known
        # value for Air_Temp and RH, so if NaN exist in Air_Temp [i], then RH cannot
        # be interpolated. 
        data_rh = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        data_temp = qaqc_file_airTemp['Air_Temp'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 8
        max_hours = 3
        qaqc_8, flags_8 = qaqc_functions.interpolate_RH_qaqc(qaqc_arr[var], data_rh, data_temp, flag, max_hours)
        qaqc_arr[var] = qaqc_8
        
        #%% One more pass with removal of outlier values to correct for
        # interpolation-related issues:
        
        #%% Remove values above 100 or below 5% threshold
        # above 100%
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 2
        threshold = 100 # in %
        qaqc_2, flags_2 = qaqc_functions.reset_max_threshold(qaqc_arr[var], data, flag, threshold)
        qaqc_arr[var] = qaqc_2
        
        # below 5%
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 2
        threshold = 5 # in %
        qaqc_2, flags_2 = qaqc_functions.reset_min_threshold(qaqc_arr[var], data, flag, threshold)
        qaqc_arr[var] = qaqc_2
        
        #%% Apply static range test (remove values where difference is > than value)
        # Maximum value between each step: 85%
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 1
        step_size = 85 # in %
        qaqc_1, flags_1 = qaqc_functions.static_range_test(qaqc_arr[var], data, flag, step_size)
        qaqc_arr[var] = qaqc_1
       
        #%% merge flags together into large array, with comma separating multiple
        # flags for each row if these exist
        flags = pd.concat([flags_1,flags_2,flags_3,flags_6,flags_8],axis=1)
        qaqc_arr['RH_flags'] = flags.apply(qaqc_functions.merge_row, axis=1)
        
        #%% plot raw vs QA/QC
        fig, ax = plt.subplots()
        plt.axhline(y=0, color='k', linestyle='-', linewidth=0.5) # plot horizontal line at 0
    
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],raw, '#1f77b4', linewidth=1) # blue
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],qaqc_8.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#d62728', linewidth=1)
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],qaqc_1.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#ff7f0e', linewidth=1)
        
        plt.title(sql_name + ' %s QA-QC WTYR %d-%d' %(var_name, yr_range[k],yr_range[k]+1))
        plt.savefig('%s %s Final Comparison WTYR %d-%d.png' %(sql_name,var_name_short,yr_range[k],yr_range[k]+1), dpi=400)
        plt.close()
        plt.clf()
        
        #%% append to qaqc_arr_final after every k iteration
        qaqc_arr_final.append(qaqc_arr.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)])
    
    #%% push qaqced variable to SQL database
    # as above, skip iteration if all air_temp is null
    if sql_file[var].isnull().all() or dt_yr.size == 0:
        continue
    # otherwise, if data (most stations), keep running
    else:
        print('# Writing newly qaqced data to SQL database #') 
        qaqc_arr_final = pd.concat(qaqc_arr_final) # concatenate lists
        sql_qaqc_name = 'qaqc_' + wx_stations_name[l]
        qaqced_array = pd.concat([qaqc_arr_final['DateTime'],qaqc_arr_final['RH'],qaqc_arr_final['RH_flags']],axis=1)
        
        # import current qaqc sql db and find columns matching the qaqc variable here
        existing_qaqc_sql = pd.read_sql('SELECT * FROM %s' %sql_qaqc_name, engine)
        colnames = existing_qaqc_sql.columns
        col_positions = [i for i, s in enumerate(colnames) if var in s]
        
        # push newly qaqced variable to SQL database -
        # move the qaqc columns into the appropriate columns in existing qaqc sql database
        existing_qaqc_sql[colnames[col_positions]] = pd.concat([qaqced_array['RH'],qaqced_array['RH_flags']],axis=1)
        existing_qaqc_sql.to_sql(name='%s' %sql_qaqc_name, con=engine, if_exists = 'replace', index=False)
        
        # make sure you assign 'DateTime' column as the primary column
        with engine.connect() as con:
                con.execute('ALTER TABLE `qaqc_%s`' %wx_stations_name[l] + ' ADD PRIMARY KEY (`DateTime`);')
