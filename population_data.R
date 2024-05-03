# Libraries -----------------------------------------------------------------
library(tidyverse)
library(psrcelmer)
library(readxl)
library(sf)
library(openxlsx)

source("functions.R")

# Basic Inputs ------------------------------------------------------------

# Silence the dplyr summarize message
options(dplyr.summarise.inform = FALSE)

# Population Years from OFM Parcelization
ofm_years <- c(2010, 2023)

# Make sure these match the boundary definition you want the data to be based on
rgc_title <- "regional_growth_center_2024_04_23"
mic_title <- "manufacturing_industrial_center_2024_04_23"
rgeo_title <- "regional_geography_class_2022"
hct_title <- "hct_station_area_vision_2050"

# Centers Names & Types ---------------------------------------------------
print(str_glue("Loading County layer from ElmerGeo to join with RGCs and MICs for county."))
county <- st_read_elmergeo("county_background") |> filter(psrc==1) |> select(county="county_nm") |> st_make_valid()

print(str_glue("Loading RGCs layer from ElmerGeo and cleaning up names."))
rgc <- suppressWarnings(st_read_elmergeo("urban_centers") |> 
                          st_make_valid() |>
                          st_centroid() |>
                          select("name", "category") |> 
                          mutate(name = str_replace_all(name, "Greater Downtown Kirkland", "Kirkland Greater Downtown")))

print(str_glue("Intersecting RGCs and County layer to add county name to RGC list."))
rgc <- suppressWarnings(st_intersection(rgc, county)|> st_drop_geometry() |> arrange(county, name))

print(str_glue("Creating RGC names for use in factors and filters"))
rgc_king <- rgc |> filter(county == "King") |> select("name") |> pull()
rgc_kitsap <- rgc |> filter(county == "Kitsap") |> select("name") |> pull()
rgc_pierce <- rgc |> filter(county == "Pierce") |> select("name") |> pull()
rgc_snohomish <- rgc |> filter(county == "Snohomish") |> select("name") |> pull()
rgc_metro <-  rgc |> filter(category == "Metro") |> select("name") |> pull()
rgc_urban <-  rgc |> filter(category == "Urban") |> select("name") |> pull()
rgc_names <- c("Regional Growth Centers", 
               "King County RGCs", rgc_king, 
               "Kitsap County RGCs", rgc_kitsap, 
               "Pierce County RGCs", rgc_pierce, 
               "Snohomish County RGCs", rgc_snohomish, 
               "Metro Growth Centers", "Urban Growth Centers", 
               "Not in a Regional Growth Center")

print(str_glue("Loading MICs layer from ElmerGeo and cleaning up names."))
mic <- suppressWarnings(st_read_elmergeo("micen") |> 
                          st_make_valid() |>
                          st_centroid() |>
                          select(name="mic", "category") |>
                          mutate(name = str_replace_all(name, "Puget Sound Industrial Center- Bremerton", "Puget Sound Industrial Center - Bremerton")))

print(str_glue("Intersecting MICs and County layer to add county name to RGC list."))
mic <- suppressWarnings(st_intersection(mic, county) |> st_drop_geometry()|> arrange(county, name))

print(str_glue("Creating MIC names for use in factors and filters"))
mic_king <- mic |> filter(county == "King") |> select("name") |> pull()
mic_kitsap <- mic |> filter(county == "Kitsap") |> select("name") |> pull()
mic_pierce <- mic |> filter(county == "Pierce") |> select("name") |> pull()
mic_snohomish <- mic |> filter(county == "Snohomish") |> select("name") |> pull()
mic_employment <-  mic |> filter(category == "Employment") |> select("name") |> pull()
mic_growth <-  mic |> filter(category == "Growth") |> select("name") |> pull()
mic_names <- c("Manufacturing Industrial Centers",
               "King County MICs", mic_king, 
               "Kitsap County MICs", mic_kitsap, 
               "Pierce County MICs", mic_pierce, 
               "Snohomish County MICs", mic_snohomish, 
               "Industrial Employment Centers", "Industrial Growth Centers",
               "Not in a Manufacturing Industrial Centers") 

print(str_glue("Creating the full list of Centers, Region, County and Urban Growth Areas for final factoring"))
county_names <- c("Region Total", paste(county |> st_drop_geometry() |> select("county") |> arrange(county) |> pull(), "County Total"))
uga_names <- c("UGA Total", "King County UGA", "Kitsap County UGA", "Pierce County UGA", "Snohomish County UGA")
all_names <- c(rgc_names, mic_names, county_names, uga_names)

rm(county, rgc, mic)

# Population and Housing Data for Centers ---------------------------------------------
population_housing <- process_ofm_data_for_centers(yrs=ofm_years)
create_ofm_summary_spreadsheet(tbl = population_housing, metric_ids = "1")

# Population and Housing Data near HCT and Centers ------------------------
population_housing_hct <- process_ofm_hct_data_for_centers(yrs=ofm_years)
create_ofm_hct_summary_spreadsheet(tbl=population_housing_hct, metric_ids = "11")
