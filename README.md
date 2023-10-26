<p align="justify">
  
##  Code Repository

This is script repository i created during my work. I include some documentation and image without adding the confidential files. 
**DISCLAIMER:** i am not using any actual data and removing sensitive data.  
## Requirements

* Python 3.10
* Others are included in requirements.txt
## Installation

##### Clone the repository and move into it
```
git clone "https://github.com/mohgavin/code-repository"
cd code-repository
python3 -m venv .pyhton-3.10
source .python-3.10/bin/activate
pip3 install requirements.txt

```

## Description

[forecast](https://github.com/mohgavin/code-repository/tree/main/forecast) --> These are script collection of forecast traffic/active user model (ARIMA, SARIMA, Holt-Winters, and Prophet). Multi process (usage on core/thread) helps to enable parallelization of forecasting so it can speed up the computation (Comparison : 7 Hours Single Threads vs 1 Hours With 12 Multi Core and Threads). The impact of holiday or seasonality trend can be seen with the chart seen below and the different between actual and forecast data is differentiated. 

<p align="center">
  <img src="https://github.com/mohgavin/code-repository/blob/main/picture/AB%20Testing%20-%20ARIMA%20vs%20Prophet.png">
</p>

<p align="center">
  <img src="https://github.com/mohgavin/code-repository/blob/main/picture/Histogram%20-%20Week35%20Delta%20Forecast%20with%20Actual.png">
</p>

[calculate ISD.ipynb](https://github.com/mohgavin/code-repository/blob/main/calculate%20ISD.ipynb) --> Algorithm to get multiple nearest point/polygon in CRS 3857, Maximal distance is needed. The nearest_sjoin function from geopandas is not enough for my use case. Consider to refork the geopandas github and contribute to the library. I also created linestring to connect nearest dots to know its distance and display it in geographic information systems.

[create Buffer Area.ipynb](https://github.com/mohgavin/code-repository/blob/main/create%20Buffer%20Area.ipynb)--> This scripts is to create buffer/polygon area around sites and used for limiting the samples to be inside and intersects with polygon. 

<p align="center">
  <img src="https://github.com/mohgavin/code-repository/blob/main/picture/ISD%20-%20LineString.png" width="300" height="300">
  <img src="https://github.com/mohgavin/code-repository/blob/main/picture/inbuilding%20-%20polygon.png" width="300" height="300">
</p>

[create_grid.ipynb](https://github.com/mohgavin/code-repository/blob/main/create_grid.ipynb) --> Algorithm to create custom rectangle grid in CRS 4326. These algorithm requires high memory and computation. Consider to revisit the algorithm in the future and simplified the steps.

[dask-process_CR_XLArea.py](https://github.com/mohgavin/code-repository/blob/main/dask-process_CR_XLArea.py) --> Script collection to query and intersects point inside polygon of MRT Route of Senayan and Bundaran HI. These are meant to collect MR at underground levels. 

[big Query](https://github.com/mohgavin/code-repository/tree/main/bigquery) --> Script collection to query Big Query SQL from Cell Rebel Crowdsource. It requires JSON or Credential from Application Default Credential of Google Cloud to get the data. To get application default credential, you can access from [here](https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to). 

[big_Data_Scripts](https://github.com/mohgavin/code-repository/tree/main/big_Data_Scripts) --> Script collection to process Smartphone UE Location/ Signal Power / Throughput / Signal Quality / Measurement Report (duration : 1 month - 3 months, approximate : Hundred of Gb to Tb)  from Raw data. These data have spatial data which can categorized into Space / Spatial Classification such as Province/Kelurahan/Kecamatan level.  Result sample are listed below. I use pandas, geopandas, dask, py-spark, sedona, airflow and combined to automatically process the data. 

[parsing](https://github.com/mohgavin/code-repository/tree/main/parsing) --> Script Collection to get bad grid/cellID of signal/throughput from output of big_Data_Script.  These requires distance from nearest sites or network element, and filter with condition only less than -105dBm or/and number of sample. The output can be configurable from mapinfo/vector files to csv/xlsx/json files.  

<p align="center">
  <img src="https://github.com/mohgavin/code-repository/blob/main/picture/Jaksel%20-%20Signal%20Power.png" width="400" height="400">
  <img src="https://github.com/mohgavin/code-repository/blob/main/picture/GBK%20-%20Throughput%20Power.png" width="400" height="400">
  <img src="https://github.com/mohgavin/code-repository/blob/main/picture/Jaksel%20-%20Population%20Map.png" width="400" height="400">
</p>

[database_process](https://github.com/mohgavin/code-repository/tree/main/database_process) --> I created this code to have better display of showing radio equipment per sector. I build executable files in Windows to be worked in other computer and anyone can run the script without having to install environment in the computer. 




 </p>
