# Clowder Geo NetCDF Extractor

Overview

This extractor uses python NetCDF4 and matplotlib to plot data from 
.nc and .nc4 files on a map.

NOTE - this is supposed to be a general purpose extractor that should work on 
many files, but because NetCDF is a flexible file format, it is not guaranteed to work.
If the data is a time series, it will generate 4 previews spaced evenly throughout the time interval.


## Build a docker image
      docker build -t clowder/ncsa-netcdf-extractor:latest .

## Test the docker container image:
      docker run --name=ncsa-netcdf-extractor -d --restart=always -e 'RABBITMQ_URI=amqp://user1:pass1@rabbitmq.ncsa.illinois.edu:5672/clowder-dev' -e 'RABBITMQ_EXCHANGE=clowder' -e 'TZ=/usr/share/zoneinfo/US/Central' -e 'REGISTRATION_ENDPOINTS=http://dts-dev.ncsa.illinois.edu:9000/api/extractors?key=key1' clowder/ncsa-netcdf-extractor

## To run without docker

1. Install required python packages using *conda*

   `conda env create -f environment.yml`
2. Activate conda environment
   `conda activate netcdf-preview`
3. Start extractor

   `./ncsa.geo.netcdf.py`
