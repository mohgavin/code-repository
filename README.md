#  Code Repository

This is script repository i created during my work. I include some documentation and image without adding the confidential files.
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

[calculate ISD.ipynb](https://github.com/mohgavin/code-repository/blob/main/calculate%20ISD.ipynb) --> Algorithm to get multiple nearest point/polygon in CRS 3857, Maximal distance is needed. I would like this to be added in the geopandas as nearest_sjoin is not enough for my use case. Will refork the geopandas github and merge them.  

[create Buffer Area.ipynb](https://github.com/mohgavin/code-repository/blob/main/create%20Buffer%20Area.ipynb)--> This scripts is to create buffer/polygon area around sites and limiting the samples to be inside and intersects with polygon. 

[create_grid.ipynb](https://github.com/mohgavin/code-repository/blob/main/create_grid.ipynb) --> Algorithm to create custom rectangle grid in CRS 4857. These algorithm requires high memory and computation. Maybe will revisit the algorith in the future and simplified the steps.

