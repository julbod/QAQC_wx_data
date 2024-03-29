# This code attempts to QA/QC the SWE data in a full year for all 
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
wx_stations = [x for x in wx_stations if not "legacy_ontree" in x] # remove legacy data for Cairnridgerun
wx_stations = [x for x in wx_stations if not "clean_eastbuxton" in x] # remove machmell from list
wx_stations = [x for x in wx_stations if not "steph" in x] # remove all stephanies
wx_stations = [x for x in wx_stations if not "datlamen" in x] # remove rennell from list
wx_stations = [x for x in wx_stations if not "cainridgerun" in x] # remove rennell from list
wx_stations = [x for x in wx_stations if not "rennellpass" in x] # remove rennell from list
wx_stations = [x for x in wx_stations if not "mountcayley" in x] # remove rennell from list
wx_stations = [x for x in wx_stations if not "mountmaya" in x] # remove rennell from list
wx_stations = [x for x in wx_stations if not "russell" in x] # remove rennell from list
wx_stations = [x for x in wx_stations if not "plummer" in x] # remove plummerhut from list
wx_stations = [w.replace('clean_machmellkliniklini', 'clean_Machmellkliniklini') for w in wx_stations] # rename machmellkliniklini so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "machmell" in x] # remove machmell from list
wx_stations = [w.replace('clean_Machmellkliniklini', 'clean_machmellkliniklini') for w in wx_stations] # rename machmellkliniklini back to original
wx_stations_name = list(map(lambda st: str.replace(st, 'clean_', ''), wx_stations)) # remove 'clean_' for csv export
wx_stations_name_cap = [wx_name.capitalize() for wx_name in wx_stations_name] # capitalise station name

#%% Loop over each station at a time and clean up the SWE variable
for l in range(len(wx_stations_name)):
    sql_database = wx_stations_name[l]
    sql_name = wx_stations_name_cap[l]
    
    print('###### Cleaning SWE data for station: %s ######' %(sql_name))     
    
    # create new directory on Windows (if does not exist) and cd into it
    Path("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/v2/individual_figures/" + sql_database + "/SWE").mkdir(parents=True, exist_ok=True)
    os.chdir("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/v2/individual_figures/" + sql_database + "/SWE")
    
    #%% import current data on SQL database and clean name of some columns to match
    # CSV column names
    sql_file = pd.read_sql(sql="SELECT * FROM clean_" + sql_database, con = engine)
   
    # make sure you only go as far as specific date for all wx stations for current water year
    # Mt Maya went offline in Nov 2024
    sql_file_idx_latest = int(np.flatnonzero(sql_file['DateTime'] == '2024-02-19 06:00:00')+1) # arbitrary date
    sql_file = sql_file[:sql_file_idx_latest]
  
    #%% Make sure there is no gap in datetime (all dates are consecutive) and place
    # nans in all other values if any gaps are identified
    df_dt = pd.Series.to_frame(sql_file['DateTime'])    
    sql_file = sql_file.set_index('DateTime').asfreq('1H').reset_index()
    dt_sql = pd.to_datetime(sql_file['DateTime'])
    
    #%% select variable you want to QA/QC
    var = 'SWE'
    var_name = 'SWE'
    var_name_short = 'SWE'
    
    #%% Loop over all years of weather station data and apply a QA/QC routine
    # get year range of dataset and loop through each year if these contain a full
    # 12-month water year && make sure you only include the previous water year
    if 10 <= datetime.now().month and datetime.now().month <= 12:
        yr_range = np.arange(dt_sql[0].year, datetime.now().year+1) # find min and max years
    else: 
        yr_range = np.arange(dt_sql[0].year, datetime.now().year) # find min and max years
        
    if wx_stations_name[l] == 'claytonfalls':
        delete = [np.flatnonzero(yr_range == 2014),np.flatnonzero(yr_range == 2016),np.flatnonzero(yr_range == 2018)]
        yr_range = np.delete(yr_range, delete)
        
    if wx_stations_name[l] == 'mountarrowsmith':
        yr_range = np.delete(yr_range, np.flatnonzero(yr_range == 2016))      
        
    if wx_stations_name[l] == 'tetrahedron':
        yr_range = np.delete(yr_range, np.flatnonzero(yr_range == 2016))      
        
    if wx_stations_name[l] == 'lowercain':
        yr_range = np.delete(yr_range, np.flatnonzero(yr_range == 2018)) 

    qaqc_arr_final = []
    for k in range(len(yr_range)):
        print('## Cleaning data for year: %d-%d ##' %(yr_range[k],yr_range[k]+1)) 
    
        # find indices of water years
        start_yr_sql = qaqc_functions.nearest(dt_sql, datetime(yr_range[k], 10, 1))
        end_yr_sql = qaqc_functions.nearest(dt_sql, datetime(yr_range[k]+1, 9, 30, 23, 00, 00))
    
        # select data for the whole water year based on datetime object
        dt_yr = np.concatenate(([np.where(dt_sql == start_yr_sql), np.where(dt_sql == end_yr_sql)]))

        # only calculate summer period for all previous water years but not for
        # current water year               
        if yr_range[k] != 2023:
            dt_summer_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 23, 00, 00, 00)))]))
    
        # store for plotting
        raw = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        qaqc_arr = sql_file.copy() # array to QAQC
                
        #%% add temporary fix to specific wx stations
        if wx_stations_name[l] == 'lowercain':
            idx_last = int(np.flatnonzero(qaqc_arr['DateTime'] == '2024-01-14 14:00:00'))
            if idx_last in qaqc_arr.index:
                qaqc_arr[var].loc[idx_last:] = np.nan
        
        #%% Apply static range test (remove values where difference is > than value)
        # Maximum value between each step: 10 degrees
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 1
        step_size = 20 # in cm
        qaqc_1, flags_1 = qaqc_functions.static_range_test(qaqc_arr[var], data, flag, step_size)
        qaqc_arr[var] = qaqc_1
        
        # add fix to LowerCain 2023-23 which is failing to identify outliers in Jan 2023
        if wx_stations[l] == 'clean_lowercain' and yr_range[k] == 2022:
            idx_first = int(np.flatnonzero(qaqc_arr['DateTime'] == '2023-01-05 21:00:00'))
            idx_last = int(np.flatnonzero(qaqc_arr['DateTime'] == '2023-01-28 15:00:00'))
            qaqc_arr[var].iloc[idx_first:idx_last] = np.nan
            qaqc_arr["SWE"].iloc[idx_first:idx_last] = np.nan
            flags_1.iloc[idx_first:idx_last] = 1
            
        # add fix to Tetrahedron which is failing to identify outliers in Spring 2021
        if wx_stations[l] == 'clean_tetrahedron' and yr_range[k] == 2021 or wx_stations[l] == 'clean_tetrahedron' and yr_range[k] == 2022:
            # first interval
            idx_first = int(np.flatnonzero(qaqc_arr['DateTime'] == '2022-04-01 18:00:00'))
            idx_last = int(np.flatnonzero(qaqc_arr['DateTime'] == '2022-04-06 13:00:00'))
            qaqc_arr[var].iloc[idx_first:idx_last] = np.nan
            qaqc_arr["SWE"].iloc[idx_first:idx_last] = np.nan
            flags_1.iloc[idx_first:idx_last] = 1
            
            # second interval
            idx_first = int(np.flatnonzero(qaqc_arr['DateTime'] == '2022-05-16 11:00:00'))
            idx_last = int(np.flatnonzero(qaqc_arr['DateTime'] == '2022-06-14 03:00:00'))
            qaqc_arr[var].iloc[idx_first:idx_last] = np.nan
            qaqc_arr["SWE"].iloc[idx_first:idx_last] = np.nan
            flags_1.iloc[idx_first:idx_last] = 1
            
            # third interval
            idx_first = int(np.flatnonzero(qaqc_arr['DateTime'] == '2023-04-19 13:00:00'))
            idx_last = int(np.flatnonzero(qaqc_arr['DateTime'] == '2023-06-12 20:00:00'))
            qaqc_arr[var].iloc[idx_first:idx_last] = np.nan
            qaqc_arr["SWE"].iloc[idx_first:idx_last] = np.nan
            flags_1.iloc[idx_first:idx_last] = 1
                
        #%% Bring timeseries back to 0 at start of water year
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 3
        qaqc_3, flags_3 = qaqc_functions.reset_zero_watyr(qaqc_arr[var], data, flag)
        qaqc_arr[var] = qaqc_3  
        
        #%% Remove all negative values (non-sensical)
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 2
        qaqc_2, flags_2 = qaqc_functions.negtozero(qaqc_arr[var], data, flag)
        qaqc_arr[var] = qaqc_2

        #%% Remove non-sensical non-zero values in summer for SWE
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 6
        
        # for all water years except current one
        if yr_range[k] != 2023: 
            summer_threshold = 12
            qaqc_6, flags_6 = qaqc_functions.SWE_summer_zeroing(qaqc_arr[var], data, flag, dt_yr, dt_summer_yr, summer_threshold, qaqc_arr['DateTime'], wx_stations_name[l], yr_range[k]+1)
            qaqc_arr[var] = qaqc_6
            
        # else for current water year
        else:
            flags_6 = qaqc_arr[var].copy()*0 # hack to keep array indices but make all vals 0
            flags_6[np.isnan(flags_6)] = 0 # make sure there are no nans
            flags_6.name = flag # change name of array from arr.copy() function        
       
        #%% one more pass to correct remaining outliers using the step size
        # and different levels until it's all 'shaved off'
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 7
        step_sizes = [15,10] # in cm
        qaqc_7, flags_7 = qaqc_functions.static_range_multiple(qaqc_arr[var], data, flag, step_sizes)
        qaqc_arr[var] = qaqc_7
        
        #%% Interpolate nans with method='linear' using pandas.DataFrame.interpolate
        # First, identify gaps larger than 3 hours (which should not be interpolated)
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 8
        max_hours = 3
        qaqc_8, flags_8 = qaqc_functions.interpolate_qaqc(qaqc_arr[var], data, flag, max_hours)
        qaqc_arr[var] = qaqc_8
        
        #%% merge flags together into large array, with comma separating multiple
        # flags for each row if these exist
        flags = pd.concat([flags_1,flags_2,flags_3,flags_6,flags_7,flags_8],axis=1)
        qaqc_arr['SWE_flags'] = flags.apply(qaqc_functions.merge_row, axis=1)
        
        # for simplicity, if flag contains flag 6 amongst other flags in one row,
        # then only keep 6 as all other flags don't matter if it's already been
        # zeroed out (i.e. flag 6 is the dominant flag)
        idx_flags6 = [i for i, s in enumerate(qaqc_arr['SWE_flags']) if '6' in s]
        qaqc_arr['SWE_flags'].iloc[idx_flags6] = '6'
        
        # if flag is both [8,7], it means the data was interpolated but the gap
        # between i and i-1 is greater than the step size required so you want
        # to remove the interpolated value and place nans (and thus only keep 7)
        idx_flags89 = [i for i, s in enumerate(qaqc_arr['SWE_flags']) if '8,9' in s]
        qaqc_arr['SWE_flags'].iloc[idx_flags89] = '9'     

        #%% plot raw vs QA/QC
        fig, ax = plt.subplots()
        plt.axhline(y=0, color='k', linestyle='-', linewidth=0.5) # plot horizontal line at 0
        
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],raw, '#1f77b4', linewidth=1) # blue
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],qaqc_8.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#0f0303', linewidth=1) # red #d62728 / black is 0f0303
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],qaqc_7.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#ff7f0e', linewidth=1)
        #ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#ff7f0e', linewidth=1)
        #ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],qaqc_9.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#0f0303', linewidth=1)
        
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
        qaqced_array = pd.concat([qaqc_arr_final['DateTime'],qaqc_arr_final['SWE'],qaqc_arr_final['SWE_flags']],axis=1)
        
        # import current qaqc sql db and find columns matching the qaqc variable here
        existing_qaqc_sql = pd.read_sql('SELECT * FROM %s' %sql_qaqc_name, engine)
        colnames = existing_qaqc_sql.columns
        col_positions = [i for i, s in enumerate(colnames) if var in s]
        
        # push newly qaqced variable to SQL database -
        # move the qaqc columns into the appropriate columns in existing qaqc sql database
        existing_qaqc_sql[colnames[col_positions]] = pd.concat([qaqced_array['SWE'],qaqced_array['SWE_flags']],axis=1)
        existing_qaqc_sql.to_sql(name='%s' %sql_qaqc_name, con=engine, if_exists = 'replace', index=False)
        
        # make sure you assign 'DateTime' column as the primary column
        with engine.connect() as con:
                con.execute('ALTER TABLE `qaqc_%s`' %wx_stations_name[l] + ' ADD PRIMARY KEY (`DateTime`);')

           