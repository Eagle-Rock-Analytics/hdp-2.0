"""paths.py

Centralized S3 bucket and directory path constants.
All bucket names are driven by environment variables with existing values as defaults,
so the pipeline can be redirected without code changes.

Environment variables
---------------------
HDP_STAGING_BUCKET  : intermediate/staging bucket for pull/clean/QAQC
                      (default: wecc-historical-wx). Legacy alias: HDP_BUCKET.
HDP_SOURCE_BUCKET   : full-history read bucket for append mode (default: staging)
HDP_PUBLISH_BUCKET  : output publish bucket (default: cadcat)
HDP_PUBLISH_PREFIX  : prefix within publish bucket (default: hdp)
"""

import os

# Staging / intermediate bucket (pull, clean, QAQC intermediates).
# Primary env var is HDP_STAGING_BUCKET; the legacy HDP_BUCKET is still honored
# for backward compatibility. Default: wecc-historical-wx (production).
# Setting HDP_STAGING_BUCKET=auto-hdp routes all clean/QAQC writes to the test
# bucket without touching production wecc-historical-wx.
BUCKET_NAME = (
    os.environ.get("HDP_STAGING_BUCKET")
    or os.environ.get("HDP_BUCKET")
    or "wecc-historical-wx"
)

# Explicit alias for call sites that reason about the staging bucket by name.
STAGING_BUCKET = BUCKET_NAME

# Full-history source bucket for append mode. Append runs read the existing
# full-record clean data from here (typically production wecc-historical-wx)
# while writing the new slice/output to BUCKET_NAME (which may be a test bucket).
# Defaults to BUCKET_NAME so non-append and same-bucket workflows are unchanged.
SOURCE_BUCKET = os.environ.get("HDP_SOURCE_BUCKET", BUCKET_NAME)

# Publish bucket (merge output: s3://{PUBLISH_BUCKET}/{PUBLISH_PREFIX}/{NETWORK}/{STATION}.zarr)
PUBLISH_BUCKET = os.environ.get("HDP_PUBLISH_BUCKET", "cadcat")
PUBLISH_PREFIX = os.environ.get("HDP_PUBLISH_PREFIX", "hdp")

# S3 directory prefixes (no trailing slashes)
MAPS_DIR = "0_maps"
RAW_WX = "1_raw_wx"
CLEAN_WX = "2_clean_wx"
CLEAN_APPEND = "_append"  # sub-prefix for append-mode new-slice .nc files
QAQC_WX = "3_qaqc_wx_v2"
QAQC_APPEND = "_append"  # sub-prefix for append-mode new-slice QAQC .zarr stores
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
