This folder contains codes designed to qaqc weather station data for the VIU hydromet network (http://graph.viu-hydromet-wx.ca/)

# QAQC flags by variable:

| Snow Depth Flags: | 
| ------------- |
| 0.	No qaqc required |
| 1.	Outlier removal #1 (between i and i-1): **25**  cm threshold |
| 2.	Remove negative values  |
| 3.	Remove duplicate consecutive values |
| 4.	Remove outliers based on mean and **4**x standard deviation over rolling window of 1 month |
| 6.	Set values to zero in summer season |
| 7.	Outlier removal #2 (between i and i-1) – multiple thresholds: **20**, **15**, **10**, **5** cm |
| 8.	Interpolation of NULL/NaN values for gaps smaller than or equal to 3 hours |

SWE Flags: 
0.	No qaqc required
1.	Outlier removal #1 (between i and i-1): 20 mm threshold
2.	Remove negative values 
6.	Set values to zero in summer season
7.	Outlier removal #2 (between i and i-1) – multiple thresholds: 15, 10 mm
8.	Interpolation of NULL/NaN values for gaps smaller than or equal to 3 hours

Air Temp Flags: 
0.	No qaqc required
1.	Outlier removal #1 (between i and i-1): 10 degrees C threshold
3.	Remove duplicate consecutive values
4.	Remove outliers based on mean and 4x standard deviation over rolling window of 1 month
6.	Convert value 0 to NULL/NaN when a value of 0 is bounded on either side by +/- 3 degrees C (this filters out values where sensor was faulty and defaulted to 0 for no reason)
8.	Interpolation of NULL/NaN values for gaps smaller than or equal to 3 hours


PC Raw Pipe Flags: 
0.	No qaqc required
1.	Outlier removal #1 (between i and i-1): 5 mm threshold
2.	Remove negative values 
3.	Reset timeseries to start at 0 at every new water year
4.	Remove outliers based on mean and 3x standard deviation over rolling window of 1 month
5.	Bring data back up to latest valid data point prior to draining of precipitation pipe
6.	Convert value 0 to NULL/NaN when a value of 0 is bounded on either side by +/- 15 mm (this filters out values where sensor was faulty and defaulted to 0 for no reason)
7.	Outlier removal #2 (between i and i-1) – multiple thresholds: 7, 6 mm
9.	Rescaled timeseries to account for evaporation of precip pipe during hot weather by identifying negative differences in the data and rescaling it back to the min/max of original trend
8.	Interpolation of NULL/NaN values for gaps smaller than or equal to 3 hours

RH Flags: 
0.	No qaqc required
1.	Outlier removal #1 (between i and i-1): 85% threshold
2.	Remove values above 100% or below 5% 
3.	Remove duplicate consecutive values equal to 100% or 0% for window size of 120 hours and 2 hours respectively
6.	Convert value 0 to NULL/NaN when a value of 0 is bounded on either side by +/- 75% (this filters out values where sensor was faulty and defaulted to 0 for no reason)
8.	Interpolation of NULL/NaN values for gaps smaller than or equal to 3 hours. RH data is first converted to vapour pressure using the qaqced Air_Temp data.

BP Flags: 
0.	No qaqc required
2.	Remove values above 120kpa or below 25kpa 
1.	Outlier removal #1 (between i and i-1): 4 kpa threshold
4.	Remove outliers based on mean and 4x standard deviation over rolling window of 1 month
8.	Interpolation of NULL/NaN values for gaps smaller than or equal to 3 hours. 

PP_Tipper Flags: 
0.	No qaqc required
1.	Outlier removal #1 (between i and i-1): 12mm threshold
2.	Remove negative values 
3.	Remove duplicate consecutive values equal to 0 mm for window size of 1000 hours
8.	Interpolation of NULL/NaN values for gaps smaller than or equal to 3 hours. 


# QAQC known issues regarding offsets, issues with qaqc worklfow, or common issue with the data:

Below are some examples where the qaqc process has either not worked well or there are offset in the data which have not been corrected for due to lack of clear understanding of the reason behind the offset. Decisions must be made as to what to do with these offsets, and then re-run the qaqc code on the offset-corrected data. Note that this list is by no means exhaustive. There is also a table for each variable which documents the flag # and its associated meaning. 

## Snow Depth:
1.	Cainridgerun:
a.	2019-20: July-Sep
2.	Eastbuxton:
a.	2014-15
b.	2017-18
c.	2018-19
d.	2019-20
e.	2020-21
f.	2021-22
g.	2022-23
3.	Klinaklini
a.	2018-19
b.	2019-20
4.	MachmellKliniklini
a.	2016-17: Oct-Apr
5.	Cayley:
a.	2015-16
b.	2016-17: Oct-Mar
6.	Rennellpass:
a.	All years

## SWE:
1.	Tetrahedron:
    a.	2020-21: Apr-July
    b.	2022-23: Apr-June
2.	ClaytonFalls:
a.	2015-16: July-Sep
b.	2017-18: July-Sep
c.	2019-20: June-Sep
3.	Arrowsmith:
a.	2017-18: July-Sep
4.	Apelake:
a.	2019-20: Jan
b.	2022-23: Nov
5.	Klinaklini:
a.	2019-20

## Air Temp:
1.	Datlamen:
a.	All years are rounded to nearest minute (vs. clean which is every 15th min). This explains the horizontal offset. Either qaqc in 15th minute or round clean. Ask Bill, then check again the qaqc that it is not affected by that (e.g. it’s cutting off some peaks in air temp in 2016-03-30 and early April which should not be removed? But flag number is not accurate, probably due to rounding off issue!
b.	2016-17 is weird…. Check!
2.	Eastbuxton:
a.	Data for 2014-15 and 2015-16 not on qaqc plots but on png. Why?
3.	Steph3:
a.	2016-17: 2017-12 peak is wrong?

## PC Raw Pipe:
None

## RH:
1.	Rennell Pass:
a.	2012-2013: June 2012 to Feb 2013: Sensor failure? Data dips to below 80% for few months
b.	2014-2016: Dec 2014 to Apr 2016: Sensor failure? Data dips to below 60% for 1.5 years and only recovers half-way through 2016 
2.	Clayton Falls:
a.	2014-2017: Data likely erroneous for all years (2015 good at start but becomes dodgy from Feb 2015; then bad start of Water Year 2018). Remove from qaqc process and graphs?
3.	Steph3:
a.	Weird low values in Dec-Jan 2018
4.	Upper Cruickshank
a.	Weird low values Oct-Dec 2020

## BP:
1.	Homathko: Values are all around 35 kpa when they should likely be higher. Checked conversion from mv to hpa and tested the atmospheric correction but it doesn’t seem to be the answer? 


## PP_Tipper:
1.	Is the outlier removal threshold too harsh (12 mm)? Record atmospheric rivers in November 2021 (around 13-15th) was ~120mm/24hr in places near Vancouver/Abbotsford, equivalent to 5mm every hour for 24 hours. Accepting this fluctuates on an hourly basis, I doubled this value, but it appears to still be cutting off some data that may be accurate (e.g. Ape Lake 2017-18).




