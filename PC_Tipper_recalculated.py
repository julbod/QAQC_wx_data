# This code does not qaqc PC_Tipper or PP_Tipper. Rather, it takes the already
# qaqced PP_Tipper data for all water years and calculates the cumulative sum for
# a particular water year. One could qaqc the existing PC_Tipper data but this is 
# sporadic throughout the database and would require qaqc-ing to fix issues with the
# original PP_Tipper data it was calculated on. It is much more efficient to 
# recalculate this PC_Tipper from the already qaqced PP_Tipper data.

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
wx_stations = [w.replace('clean_steph3', 'clean_Stephanie3') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [w.replace('clean_steph6', 'clean_Stephanie6') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "steph" in x] # remove all stephanies
wx_stations = [x for x in wx_stations if not "clean_eastbuxton_archive" in x] # remove all stephanies
wx_stations = [x for x in wx_stations if not "russell" in x] # remove rennell from list
wx_stations = [w.replace('clean_Stephanie3', 'clean_steph3') for w in wx_stations] # rename steph3 back to original
wx_stations = [w.replace('clean_Stephanie6', 'clean_steph6') for w in wx_stations] # rename steph6 back to original
wx_stations_name = list(map(lambda st: str.replace(st, 'clean_', ''), wx_stations)) # remove 'clean_' for csv export
wx_stations_name_cap = [wx_name.capitalize() for wx_name in wx_stations_name] # capitalise station name

#%% Loop over each station at a time and clean up the air temperature variable
for l in range(len(wx_stations_name)): 
    sql_database = wx_stations_name[l]
    sql_name = wx_stations_name_cap[l]
    
    print('###### Producing Tipper Cummulative data for station: %s ######' %(sql_name))     
    
    # create new directory on Windows (if does not exist) and cd into it
    Path("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/v2/individual_figures/" + sql_database + "/BP").mkdir(parents=True, exist_ok=True)
    os.chdir("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/v2/individual_figures/" + sql_database + "/BP")
    
    #%% import current data on SQL database and clean name of some columns to match
    # CSV column names
    sql_file = pd.read_sql(sql="SELECT * FROM qaqc_" + sql_database, con = engine)
    
    #%% Make sure there is no gap in datetime (all dates are consecutive) and place
    # nans in all other values if any gaps are identified
    df_dt = pd.Series.to_frame(sql_file['DateTime'])    
    sql_file = sql_file.set_index('DateTime').asfreq('1H').reset_index()
    dt_sql = pd.to_datetime(sql_file['DateTime'])
    
    #%% select variable you want to QA/QC
    var = 'PP_Tipper'
    var_name = 'PP_Tipper'
    var_name_short = 'PP_Tipper'
    
    if 10 <= datetime.now().month and datetime.now().month <= 12:
        yr_range = np.arange(dt_sql[0].year, datetime.now().year+1) # find min and max years
    elif wx_stations_name[l] == 'machmell': 
        yr_range = np.arange(dt_sql[0].year, datetime.now().year-1) # find min and max years
    else: 
        yr_range = np.arange(dt_sql[0].year, datetime.now().year) # find min and max years
        
    PC_Tipper_final = []
    for k in range(len(yr_range)):
        print('## Cleaning data for year: %d-%d ##' %(yr_range[k],yr_range[k]+1)) 
    
        # find indices of water years
        start_yr_sql = qaqc_functions.nearest(dt_sql, datetime(yr_range[k], 10, 1))
        end_yr_sql = qaqc_functions.nearest(dt_sql, datetime(yr_range[k]+1, 9, 30, 23, 00, 00))
    
        # select data for the whole water year based on datetime object
        dt_yr = np.concatenate(([np.where(dt_sql == start_yr_sql), np.where(dt_sql == end_yr_sql)]))
        
        # store for plotting
        raw = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        PC_tipper = sql_file.copy()
        
        # calculate cumsum 
        data = PC_tipper[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        PC_tipper_0 = np.cumsum(data) 
        PC_tipper_0 = np.round(PC_tipper_0,2) 
        flags_0 = data.copy()*0 # hack to keep array indices but make all vals 0 for flag
        flags_0[np.isnan(flags_0)] = 0 # make sure there are no nans and if so replace by flag 0
        PC_tipper["PC_Tipper"] = PC_tipper_0
       
        #%% merge flags together into large array, with comma separating multiple
        # flags for each row if these exist
        flags = pd.concat([flags_0],axis=1)
        PC_tipper['PC_Tipper_flags'] = flags.apply(qaqc_functions.merge_row, axis=1)
        
        #%% plot raw vs QA/QC
        fig, ax = plt.subplots()
        plt.axhline(y=0, color='k', linestyle='-', linewidth=0.5) # plot horizontal line at 0
    
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],PC_tipper_0.loc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)], '#d62728', linewidth=1)
        
        plt.title(sql_name + ' PC_Tipper QA-QC WTYR %d-%d' %(yr_range[k],yr_range[k]+1))
        plt.savefig('%s Final Comparison WTYR %d-%d.png' %(sql_name,yr_range[k],yr_range[k]+1), dpi=400)
        plt.close()
        plt.clf()
        
        #%% append to qaqc_arr_final after every k iteration
        PC_Tipper_final.append(PC_tipper.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)])
        
    #%% push qaqced variable to SQL database
    # as above, skip iteration if all air_temp is null
    if sql_file[var].isnull().all() or dt_yr.size == 0:
        continue
    # otherwise, if data (most stations), keep running
    else:
        print('# Writing newly qaqced data to SQL database #') 
        PC_Tipper_final = pd.concat(PC_Tipper_final) # concatenate lists
        sql_qaqc_name = 'qaqc_' + wx_stations_name[l]
        qaqced_array = pd.concat([PC_Tipper_final['DateTime'],PC_Tipper_final['PC_Tipper'],PC_Tipper_final['PC_Tipper_flags']],axis=1)
        
        # import current qaqc sql db and find columns matching the qaqc variable here
        existing_qaqc_sql = pd.read_sql('SELECT * FROM %s' %sql_qaqc_name, engine)
        colnames = existing_qaqc_sql.columns
        col_positions = [i for i, s in enumerate(colnames) if "PC_Tipper" in s]
        
        # push newly qaqced variable to SQL database -
        # move the qaqc columns into the appropriate columns in existing qaqc sql database
        existing_qaqc_sql[colnames[col_positions]] = pd.concat([qaqced_array['PC_Tipper'],qaqced_array['PC_Tipper_flags']],axis=1)
        existing_qaqc_sql.to_sql(name='%s' %sql_qaqc_name, con=engine, if_exists = 'replace', index=False)
        
        # make sure you assign 'DateTime' column as the primary column
        with engine.connect() as con:
                con.execute('ALTER TABLE `qaqc_%s`' %wx_stations_name[l] + ' ADD PRIMARY KEY (`DateTime`);')
                