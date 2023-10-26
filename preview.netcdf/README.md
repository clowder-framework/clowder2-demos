# Clowder Geo NetCDF Extractor

Overview

This extractor uses python NetCDF4 and matplotlib to plot data from 
.nc and .nc4 files on a map.

NOTE - this is supposed to be a general purpose extractor that should work on 
many files, but because NetCDF is a flexible file format, it is not guaranteed to work.
If the data is a time series, it will generate 4 previews spaced evenly throughout the time interval.


## Build a docker image
      docker build -t ncsa-netcdf-extractor:latest .

## Test the docker container image:
      docker run -t -i --rm --net clowder_clowder -e "RABBITMQ_URI=amqp://guest:guest@rabbitmq:5672/%2f" --name "ncsa-netcdf-extractor" ncsa-netcdf-extractor

## To run without docker

1. Install required python packages using *conda*

   `conda env create -f environment.yml`
2. Activate conda environment
   `conda activate netcdf-preview`
3. Start extractor

   `./ncsa.geo.netcdf.py`
