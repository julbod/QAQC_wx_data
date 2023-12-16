# This code attempts to QA/QC the snow depth data in a full year for all wx
# stations and all years

# Written by Julien Bodart (VIU) - 12/13/2023

import os
import pandas as pd 
import os.path
from sqlalchemy import create_engine
import pymysql
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from csv import writer
import re
from pathlib import Path
import traceback
from datetime import timedelta

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
wx_stations = [x for x in wx_stations if not "rennellpass" in x] # remove rennell from list
wx_stations = [x for x in wx_stations if not "plummerhut" in x] # remove plummer from list
wx_stations = [w.replace('clean_Stephanie3', 'clean_steph3') for w in wx_stations] # rename steph3 back to original
wx_stations = [w.replace('clean_Machmellkliniklini', 'clean_machmellkliniklini') for w in wx_stations] # rename machmellkliniklini back to original
wx_stations_name = list(map(lambda st: str.replace(st, 'clean_', ''), wx_stations)) # remove 'clean_' for csv export
wx_stations_name_cap = [wx_name.capitalize() for wx_name in wx_stations_name] # capitalise station name

#%% Loop over each station at a time and clean up the snow depth variable
for l in range(len(wx_stations_name)):
    sql_database = wx_stations_name[l]
    sql_name = wx_stations_name_cap[l]
    
    print('###### Cleaning snow data for station: %s ######' %(sql_name))     
    
    # create new directory on Windows (if does not exist) and cd into it
    Path("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/v2/individual_figures/" + sql_database + "/SnowDepth").mkdir(parents=True, exist_ok=True)
    os.chdir("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/v2/individual_figures/" + sql_database + "/SnowDepth")
    
    #%% import current data on SQL database and clean name of some columns to match
    # CSV column names
    sql_file = pd.read_sql(sql="SELECT * FROM clean_" + sql_database, con = engine)
    
    #%% Make sure there is no gap in datetime (all dates are consecutive) and place
    # nans in all other values if any gaps are identified
    #deltas = sql_file['DateTime'].diff()[1:] # identify non-consecutive datetime
    #gaps = deltas[deltas > timedelta(days=1)] # spits out any gaps > one day
    df_dt = pd.Series.to_frame(sql_file['DateTime'])    
    sql_file = sql_file.set_index('DateTime').asfreq('1H').reset_index()
    dt_sql = pd.to_datetime(sql_file['DateTime'])
    
    #%% select variable you want to QA/QC
    var = 'Snow_Depth'
    var_name = 'Snow Depth'
    var_name_short = 'SnowDepth'
    
    #%% Loop over all years of weather station data and apply a QA/QC routine
    # get year range of dataset and loop through each year if these contain a full
    # 12-month water year && make sure you only include the previous water year
    if 10 <= datetime.now().month and datetime.now().month <= 12:
        yr_range = np.arange(dt_sql[0].year, datetime.now().year) # find min and max years
    else: 
        yr_range = np.arange(dt_sql[0].year, datetime.now().year-1) # find min and max years
    
    # remove specific years in arrays due to issue with data quality in 'clean'
    if wx_stations_name[l] == 'steph3':
        yr_range = np.arange(int(yr_range[np.flatnonzero(yr_range == 2015)]),yr_range[-1]+1)
        
    if wx_stations_name[l] == 'tetrahedron':
        yr_range = np.arange(int(yr_range[np.flatnonzero(yr_range == 2016)])+1,yr_range[-1]+1)
        
    if wx_stations_name[l] == 'mountcayley':
        yr_range = np.delete(yr_range, np.flatnonzero(yr_range == 2022))
        
    qaqc_arr_final = []
    
    for k in range(len(yr_range)):
        print('## Cleaning data for year: %d-%d ##' %(yr_range[k],yr_range[k]+1))      
        # calculate datetime for winter/summer and 12 months of that year
        # if exists (i.e. if there is a full 12 month water year (YYYY-10-01 to 
        # YYYY+1-09-30))
        try:
            # find summer, winter and annual datetimes
            dt_winter_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k], 10, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 31, 00, 00, 00)))]))
            dt_summer_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 23, 00, 00, 00)))]))
            dt_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k], 10, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 30, 23, 00, 00)))]))

        # if not full year - find issue and attempt to fix it if there are some 
        # days missing at start or end of water year
        except ValueError:
            err_var = traceback.format_exc() # extract error message for variable        
            print('It appears that there is no summer/winter data for year: %d-%d - checking ...' %(yr_range[k],yr_range[k]+1))
            
            # find nearest date for both summer and winter compared to typical
            # year where winter starts YYYY-10-01 and summer ends YYYY+1-09-30
            start_yr_sql = qaqc_functions.nearest(dt_sql, datetime(yr_range[k], 10, 1, 00, 00, 00))
            end_yr_sql = qaqc_functions.nearest(dt_sql, datetime(yr_range[k+1], 9, 30, 23, 00, 00))

            # if the start of the winter for this dataset is not too different 
            # than the start of the winter for a typical year (i.e. YYYY-12-15), 
            # which would indicate a lack of early winter data for this dataset,
            # still continue with the loop with slightly truncated dataset at 
            # the start
            if 'dt_winter' in err_var and start_yr_sql <= datetime(yr_range[k], 12, 15, 00, 00, 00):
                print('# Shorter winter timeseries for year: %d-%d - going ahead #' %(yr_range[k],yr_range[k]+1))
                dt_yr = np.concatenate(([np.where(dt_sql == start_yr_sql), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 30, 23, 00, 00)))]))
                dt_winter_yr = np.concatenate(([np.where(dt_sql == start_yr_sql), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 31, 23, 00, 00)))]))
                dt_summer_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 23, 00, 00, 00)))]))

            # if the end of the summer for this dataset is not too different 
            # than the end of the summer for a typical year (i.e. YYYY+1-09-30), 
            # which would indicate a lack of summer data towards the end of this 
            # dataset, still continue with the loop with slightly truncated 
            # dataset at the end
            elif 'dt_summer' in err_var and end_yr_sql >= datetime(yr_range[k+1], 7, 15, 00, 00, 00):
                print('# Shorter summer timeseries for year: %d-%d - going ahead #' %(yr_range[k],yr_range[k]+1))
                dt_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k], 10, 1, 00, 00, 00))), np.where(dt_sql == end_yr_sql)]))
                dt_summer_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 23, 00, 00, 00)))]))
            
            # else if this difference is too big for either winter or summer
            # then skip the dataset completely and go to next year
            else:
                print('Full summer/winter data for year: %d-%d is lacking - exiting loop' %(yr_range[k],yr_range[k]+1))
                # write warning to csv for inventory
                with open("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/v2/individual_figures/" + sql_database + "/SnowDepth/messages_" + sql_name + ".csv", 'a', newline='') as f_object:
                    writer_object = writer(f_object)
                    writer_object.writerow([sql_name + ' ' + var_name + ': no full summer/winter data - not QA/QCed for year:', yr_range[k],yr_range[k]+1])
                    f_object.close()
                
                # skip to next iteration if error found
                continue
        
        # likewise, if snow depth is all NaNs or there is no yearly data
        # (e.g. Datlamen) then skip iteration
        if sql_file[var].isnull().all() or dt_yr.size == 0: # if all nans or there is no yearly data
            print('It appears that there is no snow depth data for year: %d-%d - exiting loop' %(yr_range[k],yr_range[k]+1))
            
            # write warning to csv for inventory
            with open("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/v2/individual_figures/" + sql_database + "/SnowDepth/QAQC_issues_" + sql_name + ".csv", 'a', newline='') as f_object:
                writer_object = writer(f_object)
                writer_object.writerow([sql_name + ' ' + var_name + ': lack of snow - not QA/QCed for year:', yr_range[k],yr_range[k]+1])
                f_object.close()
            
            # skip to next iteration if error found
            continue  

        # store for plotting
        raw = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        qaqc_arr = sql_file.copy() # array to QAQC
                
        #%% find min value for specific interval (if needed)
        #idx_first = int(np.flatnonzero(qaqc_arr['DateTime'] == '2019-10-01 00:00:00'))
        #idx_last = int(np.flatnonzero(qaqc_arr['DateTime'] == '2019-10-14 17:00:00'))
        #round(np.mean(qaqc_arr[var].iloc[idx_first:idx_last]),2)

        #%% Apply static range test (remove values where difference is > than value)
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 1
        step_size = 25 # in cm
        qaqc_1, flags_1 = qaqc_functions.static_range_test(qaqc_arr[var], data, flag, step_size)
        qaqc_arr[var] = qaqc_1

        #%% Remove all negative values (non-sensical)
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 2
        qaqc_2, flags_2 = qaqc_functions.negtozero(qaqc_arr[var], data, flag)
        qaqc_arr[var] = qaqc_2
    
        #%% Remove duplicate consecutive values
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 3
        qaqc_3, flags_3 = qaqc_functions.duplicates(qaqc_arr[var], data, flag)
        qaqc_arr[var] = qaqc_3

        #%% Remove outliers based on mean and std using a rolling window for each
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 4
        st_dev = 4 # specify how many times you want to multiple st_dev (good starting point is 3; 1 is too harsh) 
        qaqc_4, flags_4 = qaqc_functions.mean_rolling_month_window(qaqc_arr[var], flag, dt_sql, st_dev)
        qaqc_arr[var] = qaqc_4
        
        #%% Remove any last outliers using sliding window of 20 samples and 
        # calculating the difference between the value at [i] and the mean of 
        # sliding window which should not exceed a specific value
        # Maximum value between each step: 30 cm
        #flag = 5
        #mean_sliding_val = 30 # in cm
        #window_len = 20 # previously 20
        #qaqc_5, flags_5 = qaqc_functions.mean_sliding_window(qaqc_arr[var], data, flag, window_len, mean_sliding_val)
        #qaqc_arr[var] = qaqc_5
               
        #%% Remove non-sensical non-zero values in summer for Snow Depth
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 6
        summer_threshold = 12
        qaqc_6, flags_6 = qaqc_functions.sdepth_summer_zeroing(qaqc_arr[var], data, flag, dt_yr, dt_summer_yr, summer_threshold, qaqc_arr['DateTime'], wx_stations_name[l], yr_range[k]+1)
        qaqc_arr[var] = qaqc_6
        
        #%% one more pass to correct remaining outliers using the step size
        # and different levels until it's all 'shaved off'
        data = qaqc_arr[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        flag = 7
        step_sizes = [20,15,10,5] # in cm
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
        #flags = pd.concat([flags_1,flags_2,flags_3,flags_4,flags_5,flags_6,flags_7,flags_8],axis=1)
        flags = pd.concat([flags_1,flags_2,flags_3,flags_4,flags_6,flags_7,flags_8],axis=1)
        qaqc_arr['Snow_Depth_flags'] = flags.apply(qaqc_functions.merge_row, axis=1)
        
        # for simplicity, if flag contains flag 6 amongst other flags in one row,
        # then only keep 6 as all other flags don't matter if it's already been
        # zeroed out (i.e. flag 6 is the dominant flag)
        idx_flags6 = [i for i, s in enumerate(qaqc_arr['Snow_Depth_flags']) if '6' in s]
        qaqc_arr['Snow_Depth_flags'].iloc[idx_flags6] = '6'

        #%% exceptions below for specific manual fixes to the data
        if wx_stations_name[l] == 'cainridgerun' and yr_range[k] == 2019:
            idx_err = int(np.flatnonzero(qaqc_arr['DateTime'] == '2020-02-21 03:00:00'))
            qaqc_arr[var].iloc[idx_err:dt_yr[1].item()+1] = np.nan
            qaqc_7.iloc[idx_err:dt_yr[1].item()+1] = np.nan
            qaqc_8.iloc[idx_err:dt_yr[1].item()+1] = np.nan
            qaqc_arr['Snow_Depth_flags'].iloc[idx_err:dt_yr[1].item()+1] = 1

        #%% plot raw vs QA/QC
        fig, ax = plt.subplots()
        
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],raw, '#1f77b4', linewidth=1) # blue
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],qaqc_8.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#d62728', linewidth=1)
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],qaqc_7.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#ff7f0e', linewidth=1)
        
        plt.title(sql_name + ' %s QA-QC WTYR %d-%d' %(var_name, yr_range[k],yr_range[k]+1))
        plt.savefig('%s %s Final Comparison WTYR %d-%d.png' %(sql_name,var_name_short,yr_range[k],yr_range[k]+1), dpi=400)
        plt.close()
        plt.clf()
        
        #%% append to qaqc_arr_final after every k iteration
        qaqc_arr_final.append(qaqc_arr.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)])

    #%% push qaqced variable to SQL database
    # as above, skip iteration if all snow depth is null (e.g. Datlamen)
    if sql_file[var].isnull().all() or dt_yr.size == 0:
        continue
    # otherwise, if data (most stations), keep running
    else:
        print('# Writing newly qaqced data to SQL database #') 
        qaqc_arr_final = pd.concat(qaqc_arr_final) # concatenate lists
        sql_qaqc_name = 'qaqc_' + wx_stations_name[l]
        qaqc_sDepth = pd.concat([qaqc_arr_final['DateTime'],qaqc_arr_final['Snow_Depth'],qaqc_arr_final['Snow_Depth_flags']],axis=1)
    
        # import current qaqc sql db and find columns matching the qaqc variable here
        existing_qaqc_sql = pd.read_sql('SELECT * FROM %s' %sql_qaqc_name, engine)
        colnames = existing_qaqc_sql.columns
        col_positions = [i for i, s in enumerate(colnames) if var in s]
        
        # push newly qaqced variable to SQL database -
        # warn if length of existing qaqc sql datbase is not the same as the 
        # qaqc variable from this code (this shouldn't happen in theory, but 
        # better be safe than sorry)
        if len(existing_qaqc_sql) != len(qaqc_sDepth):
            #raise Exception('Careful: lengths between existing qaqc array and new qaqc var do not match!') # Don't! If you catch, likely to hide bugs.
            # skip to next iteration if error found
            print('>>> Length difference between SQL and qaqced variable - not writing to SQL database! >>>')  
            continue
        else:
            # move the qaqc columns into the appropriate columns in existing qaqc sql database
            existing_qaqc_sql[colnames[col_positions]] = pd.concat([qaqc_sDepth['Snow_Depth'],qaqc_sDepth['Snow_Depth_flags']],axis=1)
            existing_qaqc_sql.to_sql(name='%s' %sql_qaqc_name, con=engine, if_exists = 'replace', index=False)
            
            # make sure you assign 'DateTime' column as the primary column
            with engine.connect() as con:
                    con.execute('ALTER TABLE `qaqc_%s`' %wx_stations_name[l] + ' ADD PRIMARY KEY (`DateTime`);')
                    
        #%% plot raw vs QA/QC with two subplots, one for data, oen for flags
#        # prepare colormap for plotting flags
#        flags_int = [int(x.replace(',','')[0]) for x in qaqc_arr['sDepth_flag']] # just keep first flag if there are multiple
#        categories = flags_int
#        colormap = np.array(['r', 'g', 'b'])
        
#        f, (ax0, ax1) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]})
#        
#        # plot raw and qaqced data
#        ax0.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],raw, '#1f77b4') # blue
#        ax0.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],qaqc_8.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#d62728')
#        ax0.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],qaqc_7.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#ff7f0e')
#        
#        # plot the flags and add colormap to each value in each points
#        ax1.scatter(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],flags_int[dt_yr[0].item():dt_yr[1].item()+1], s=3, c=flags_int[dt_yr[0].item():dt_yr[1].item()+1], cmap='seismic')
#        from matplotlib.cm import ScalarMappable
#        norm = plt.Normalize(np.min(flags_int[dt_yr[0].item():dt_yr[1].item()+1]), np.max(flags_int[dt_yr[0].item():dt_yr[1].item()+1]))
#        sm = ScalarMappable(cmap='seismic', norm=norm)
#        sm.set_array([])
#        
#        # Connect points with lines and use the same colormap
#        for i in range(len(flags_int[dt_yr[0].item():dt_yr[1].item()+1]) - 1):
#            ax1.plot([sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)][i],sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)][i+1]],[flags_int[dt_yr[0].item():dt_yr[1].item()+1][i],flags_int[dt_yr[0].item():dt_yr[1].item()+1][i+1]],color=sm.to_rgba(flags_int[dt_yr[0].item():dt_yr[1].item()+1][i]))
