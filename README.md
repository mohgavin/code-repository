#  Code Repository

This is script repository i created during my work. I include some documentation and image without adding the confidential files. 
Please note, for presentation, i am not using any data that represents something
## Requirements

* Python 3.10
* Others are included in requirements.txt
## Installation

##### Clone the repository and move into it
```
git clone "https://github.com/mohgavin/code-repository"
cd code-repository
```

## Description

[calculate ISD.ipynb](https://github.com/mohgavin/code-repository/blob/main/calculate%20ISD.ipynb) --> Algorithm to get multiple nearest point/polygon in CRS 3857, Maximal distance is needed. The nearest_sjoin function is not enough for my use case. Consider to refork the geopandas github and merge them.  

[create Buffer Area.ipynb](https://github.com/mohgavin/code-repository/blob/main/create%20Buffer%20Area.ipynb)--> This scripts is to create buffer/polygon area around sites and used for limiting the samples to be inside and intersects with polygon. 

[create_grid.ipynb](https://github.com/mohgavin/code-repository/blob/main/create_grid.ipynb) --> Algorithm to create custom rectangle grid in CRS 4326. These algorithm requires high memory and computation. Consider to revisit the algorithm in the future and simplified the steps.

[dask-process_CR_XLArea.py](https://github.com/mohgavin/code-repository/blob/main/dask-process_CR_XLArea.py) --> These are scripts to query and intersects point inside polygon of MRT Route of Senayan and Bundaran HI. These are meant to collect MR at underground levels. 

[Big Query](https://github.com/mohgavin/code-repository/tree/main/BigQuery) --> These are script collection to query Big Query SQL from Cell Rebel Crowdsource. It requires JSON or Credential from Application Default Credection of Google Cloud to get the data

[Big_Data_Scripts](https://github.com/mohgavin/code-repository/tree/main/Big_Data_Scripts) --> These are script collection to process big data MDT / Measurement Report from LTE 3GPP. Result sample are listed below. 

<img src="https://github.com/mohgavin/code-repository/blob/main/Picture/Jaksel%20-%20Signal%20Power.png" width="100" height="100">
