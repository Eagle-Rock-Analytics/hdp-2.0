"""paths.py

Centralized S3 bucket and directory path constants.
All bucket names are driven by environment variables with existing values as defaults,
so the pipeline can be redirected without code changes.

Environment variables
---------------------
HDP_BUCKET          : intermediate/staging bucket (default: wecc-historical-wx)
HDP_PUBLISH_BUCKET  : output publish bucket (default: cadcat)
HDP_PUBLISH_PREFIX  : prefix within publish bucket (default: hdp)
"""

import os

# Staging / intermediate bucket (pull, clean, qaqc intermediates)
BUCKET_NAME = os.environ.get("HDP_BUCKET", "wecc-historical-wx")

# Publish bucket (merge output: s3://{PUBLISH_BUCKET}/{PUBLISH_PREFIX}/{NETWORK}/{STATION}.zarr)
PUBLISH_BUCKET = os.environ.get("HDP_PUBLISH_BUCKET", "cadcat")
PUBLISH_PREFIX = os.environ.get("HDP_PUBLISH_PREFIX", "hdp")

# S3 directory prefixes (no trailing slashes)
MAPS_DIR = "0_maps"
RAW_WX = "1_raw_wx"
CLEAN_WX = "2_clean_wx"
QAQC_WX = "3_qaqc_wx_v2"
MERGE_WX = "4_merge_wx_v2"

# Commonly used full S3 URIs
STATIONS_CSV_PATH = f"s3://{BUCKET_NAME}/{CLEAN_WX}/temp_clean_all_station_list.csv"

# Map shapefiles
WECC_TERR = (
    f"s3://{BUCKET_NAME}/{MAPS_DIR}/WECC_Informational_MarineCoastal_Boundary_land.shp"
)
WECC_MAR = f"s3://{BUCKET_NAME}/{MAPS_DIR}/WECC_Informational_MarineCoastal_Boundary_marine.shp"
ASCC = f"s3://{BUCKET_NAME}/{MAPS_DIR}/Alaska_Energy_Authority_Regions.shp"
MRO = f"s3://{BUCKET_NAME}/{MAPS_DIR}/NERC_Regions_EIA.shp"
