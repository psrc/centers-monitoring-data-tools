# Libraries -----------------------------------------------------------------
library(tidyverse)
library(psrccensus)
library(psrcelmer)
library(tidycensus)
library(readxl)
library(sf)
library(openxlsx)

source("functions.R")

# Basic Inputs ------------------------------------------------------------

# Silence the dplyr summarize message
options(dplyr.summarise.inform = FALSE)

# Blockgroup data before 2013 is not available via the census API
acs_pre2013_bg_dir <- "X:/DSA/acs_blockgroup_data"
pre_api_year <- c(2010)
api_years <- c(2022)

# Population Years from OFM Parcelization
ofm_years <- c(2010, 2023)

# Make sure these match the boundary definition you want the data to be based on
rgc_title <- "Regional Growth Center (4/23/2024)"
mic_title <- "MIC (4/23/2024)"
rgeo_title <- "Regional Geography Class (2022 RTP)"

# Update Census Data or use currently processed data
download_census_data <- "No"

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

# Process ACS Data for Centers ------------------------------------------------------

if (download_census_data == "Yes") {
  
  blockgroup_splits <- NULL
  for (yr in c(pre_api_year, api_years)) {
    splits <- generate_blockgroup_splits(y = yr)
    if (is.null(blockgroup_splits)) {blockgroup_splits <- splits} else {blockgroup_splits <- bind_rows(blockgroup_splits, splits)}
    rm(splits)
  }
  
  gender <- process_acs_data_for_centers(census_metric = "Gender", split_variable = "percent_of_total_pop")
  age <- process_acs_data_for_centers(census_metric = "Age", split_variable = "percent_of_total_pop")
  race <- process_acs_data_for_centers(census_metric = "Race", split_variable = "percent_of_total_pop")
  mode_to_work <- process_acs_data_for_centers(census_metric = "Mode to Work", split_variable = "percent_of_total_pop")
  education <- process_acs_data_for_centers(census_metric = "Education", split_variable = "percent_of_total_pop")
  income <- process_acs_data_for_centers(census_metric = "Income", split_variable = "percent_of_occupied_housing_units")
  tenure <- process_acs_data_for_centers(census_metric = "Tenure", split_variable = "percent_of_occupied_housing_units")
  renter_cost_burden <- process_acs_data_for_centers(census_metric = "Renter Cost Burden", split_variable = "percent_of_occupied_housing_units")
  owner_cost_burden <- process_acs_data_for_centers(census_metric = "Owner Cost Burden", split_variable = "percent_of_occupied_housing_units")
  housing_type <- process_acs_data_for_centers(census_metric = "Housing Type", split_variable = "percent_of_housing_units")
  household_type <- process_acs_data_for_centers(census_metric = "Household Type", split_variable = "percent_of_occupied_housing_units")
  
  centers_acs_data <- bind_rows(gender, age, race, mode_to_work, education, income, tenure, renter_cost_burden, owner_cost_burden, housing_type, household_type)
  saveRDS(centers_acs_data, "output-data/centers_acs_data.rds")
  rm(gender, age, race, mode_to_work, education, income, tenure, renter_cost_burden, owner_cost_burden, housing_type, household_type)
  
} else{
  print(str_glue("Loading already processed Centers ACS data"))
  centers_acs_data <- readRDS("output-data/centers_acs_data.rds")
}

# Final Data Summary ------------------------------------------------------

# Round data for use in report
centers_acs_data <- centers_acs_data |> mutate(estimate = round(estimate, -1), moe = round(moe, -1))

# Create summary workbooks for each center
for (m in unique(centers_acs_data$metric)) {
  acs_summary_table_creation(census_metric=m, census_data=centers_acs_data)
}
