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
rgc_title <- "Regional Growth Center (12/12/2023)"
mic_title <- "MIC (1/5/2024)"
rgeo_title <- "Regional Geography Class (2022 RTP)"

elmer_rgc_column <- "regional_growth_center_2023_12_12"
elmer_mic_column <- "manufacturing_industrial_center_2024_01_05"

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

# Blockgroup Splits from Elmer --------------------------------------------
blockgroup_splits <- NULL
for (yr in c(pre_api_year, api_years)) {
  splits <- generate_blockgroup_splits(y = yr)
  if (is.null(blockgroup_splits)) {blockgroup_splits <- splits} else {blockgroup_splits <- bind_rows(blockgroup_splits, splits)}
  rm(splits)
}

# Process ACS Data for Centers ------------------------------------------------------

if (download_census_data == "Yes") {
  
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
  
  centers_acs_data <- readRDS("output-data/centers_acs_data.rds")
  
}

# Final Data Summary ------------------------------------------------------

for (m in unique(centers_acs_data$metric)) {
  
  summary_table_creation(census_metric=m, census_data=centers_acs_data)
  
}



# Population ---------------------------------------------
ofm_population <- NULL
for (y in ofm_years) {

  # Parcel population
  print(str_glue("Loading {y} OFM based parcelized estimates of total population"))
  if (y >= 2020) {ofm_vintage <- y} else {ofm_vintage <- 2020}
  q <- paste0("SELECT parcel_dim_id, estimate_year, total_pop from ofm.parcelized_saep_facts WHERE ofm_vintage = ", ofm_vintage, " AND estimate_year = ", y, "")
  p <- get_query(sql = q)

  # Parcel Dimensions RGCs and MICs
  if (y >=2018) {parcel_yr <- 2018} else {parcel_yr <- 2014}
  print(str_glue("Loading {parcel_yr} parcel dimensions from Elmer"))
  q <- paste0("SELECT parcel_dim_id, parcel_id, ", elmer_rgc_column, ", ",elmer_mic_column, " from small_areas.parcel_dim WHERE base_year = ", parcel_yr, " AND dummy_type=0")
  d <- get_query(sql = q) |> 
    rename(rgc_name = all_of(elmer_rgc_column), mic_name = all_of(elmer_mic_column)) |> 
    mutate(rgc_name = str_replace_all(rgc_name, "N/A", "Not in Center")) |>
    mutate(mic_name = str_replace_all(mic_name, "N/A", "Not in Center")) |>
    mutate(rgc_name = str_replace_all(rgc_name, "Greater Downtown Kirkland", "Kirkland Greater Downtown")) |>
    mutate(rgc_name = gsub("Bellevue", "Bellevue Downtown", rgc_name)) |>
    mutate(mic_name = gsub("Kent MIC", "Kent", mic_name)) |>
    mutate(mic_name = gsub("Paine Field / Boeing Everett", "Paine Field/Boeing Everett", mic_name)) |>
    mutate(mic_name = gsub("Sumner Pacific", "Sumner-Pacific", mic_name)) |>
    mutate(mic_name = gsub("Puget Sound Industrial Center- Bremerton", "Puget Sound Industrial Center - Bremerton", mic_name)) |>
    mutate(rgc_name = gsub("Redmond-Overlake", "Redmond Overlake", rgc_name)) |>
    mutate(rgc_name = gsub("Greater Downtown Kirkland", "Kirkland Greater Downtown", rgc_name)) 
  
  # Add MIC and RGC names to Parcels
  p <- left_join(p, d, by="parcel_dim_id") |> 
    mutate(parcel_id = replace_na(parcel_id, 0)) |>
    mutate(rgc_name = replace_na(rgc_name,"Not in Center")) |>
    mutate(mic_name = replace_na(mic_name, "Not in Center"))
  
  # Calculate Population for RGCs
  rgc <- p |> 
    group_by(rgc_name, estimate_year) |> 
    summarise(estimate = sum(total_pop)) |> 
    as_tibble() |>
    rename(year="estimate_year", name="rgc_name") |>
    mutate(center_boundary = rgc_title)
  
  # Calculate Population for All RGCs
  rgc_total <- p |> 
    filter(rgc_name != "Not in Center") |>
    group_by(estimate_year) |> 
    summarise(estimate = sum(total_pop)) |> 
    as_tibble() |>
    rename(year="estimate_year") |>
    mutate(center_boundary = rgc_title, name = "All RGCs")
  
  # Calculate Total Population
  region_rgc <- p |> 
    group_by(estimate_year) |> 
    summarise(estimate = sum(total_pop)) |> 
    as_tibble() |>
    rename(year="estimate_year") |>
    mutate(center_boundary = rgc_title, name = "Region")
  
  pop_rgc <- bind_rows(rgc, rgc_total, region_rgc) |> mutate(name = factor(name, levels = rgc_names)) |> arrange(name)
  
  # Calculate Population for MICs
  mic <- p |> 
    group_by(mic_name, estimate_year) |> 
    summarise(estimate = sum(total_pop)) |> 
    as_tibble() |>
    rename(year="estimate_year", name="mic_name") |>
    mutate(center_boundary = mic_title)
  
  # Calculate Population for All MICs
  mic_total <- p |> 
    filter(mic_name != "Not in Center") |>
    group_by(estimate_year) |> 
    summarise(estimate = sum(total_pop)) |> 
    as_tibble() |>
    rename(year="estimate_year") |>
    mutate(center_boundary = mic_title, name = "All MICs")
  
  # Calculate Total Population
  region_mic <- p |> 
    group_by(estimate_year) |> 
    summarise(estimate = sum(total_pop)) |> 
    as_tibble() |>
    rename(year="estimate_year") |>
    mutate(center_boundary = mic_title, name = "Region")
  
  pop_mic <- bind_rows(mic, mic_total, region_mic) |> mutate(name = factor(name, levels = mic_names)) |> arrange(name)
  
  if (is.null(ofm_population)) {ofm_population <- bind_rows(pop_rgc, pop_mic)} else {ofm_population <- bind_rows(ofm_population, pop_rgc, pop_mic)}
  
  rm(p, q, d, pop_rgc, rgc, rgc_total, region_rgc, pop_mic, mic, mic_total, region_mic)

}

print(str_glue("Final Cleanup for Population data"))
ofm_population <- ofm_population  |> mutate(estimate = as.integer(estimate), metric = "Total Population", grouping = "All")

# Housing Units ---------------------------------------------
ofm_housing_units <- NULL
for (y in ofm_years) {
  
  # Parcel Hosuing
  print(str_glue("Loading {y} OFM based parcelized estimates of total housing units"))
  if (y >= 2020) {ofm_vintage <- y} else {ofm_vintage <- 2020}
  q <- paste0("SELECT parcel_dim_id, estimate_year, housing_units from ofm.parcelized_saep_facts WHERE ofm_vintage = ", ofm_vintage, " AND estimate_year = ", y, "")
  p <- get_query(sql = q)
  
  # Parcel Dimensions RGCs and MICs
  if (y >=2018) {parcel_yr <- 2018} else {parcel_yr <- 2014}
  print(str_glue("Loading {parcel_yr} parcel dimensions from Elmer"))
  q <- paste0("SELECT parcel_dim_id, parcel_id, ", elmer_rgc_column, ", ",elmer_mic_column, " from small_areas.parcel_dim WHERE base_year = ", parcel_yr, " AND dummy_type=0")
  d <- get_query(sql = q) |> 
    rename(rgc_name = all_of(elmer_rgc_column), mic_name = all_of(elmer_mic_column)) |> 
    mutate(rgc_name = str_replace_all(rgc_name, "N/A", "Not in Center")) |>
    mutate(mic_name = str_replace_all(mic_name, "N/A", "Not in Center")) |>
    mutate(mic_name = gsub("Cascade", "Cascade Industrial Center - Arlington/Marysville", mic_name)) |>
    mutate(rgc_name = gsub("Bellevue", "Bellevue Downtown", rgc_name)) |>
    mutate(mic_name = gsub("Kent MIC", "Kent", mic_name)) |>
    mutate(mic_name = gsub("Paine Field / Boeing Everett", "Paine Field/Boeing Everett", mic_name)) |>
    mutate(mic_name = gsub("Sumner Pacific", "Sumner-Pacific", mic_name)) |>
    mutate(mic_name = gsub("Puget Sound Industrial Center- Bremerton", "Puget Sound Industrial Center - Bremerton", mic_name)) |>
    mutate(rgc_name = gsub("Redmond-Overlake", "Redmond Overlake", rgc_name)) |>
    mutate(rgc_name = gsub("Greater Downtown Kirkland", "Kirkland Greater Downtown", rgc_name)) 
  
  # Add MIC and RGC names to Parcels
  p <- left_join(p, d, by="parcel_dim_id") |> 
    mutate(parcel_id = replace_na(parcel_id, 0)) |>
    mutate(rgc_name = replace_na(rgc_name,"Not in Center")) |>
    mutate(mic_name = replace_na(mic_name, "Not in Center"))
  
  # Calculate Housing Units for RGCs
  rgc <- p |> 
    group_by(rgc_name, estimate_year) |> 
    summarise(estimate = sum(housing_units)) |> 
    as_tibble() |>
    rename(year="estimate_year", name="rgc_name") |>
    mutate(center_boundary = rgc_title)
  
  # Calculate Housing Units for All RGCs
  rgc_total <- p |> 
    filter(rgc_name != "Not in Center") |>
    group_by(estimate_year) |> 
    summarise(estimate = sum(housing_units)) |> 
    as_tibble() |>
    rename(year="estimate_year") |>
    mutate(center_boundary = rgc_title, name = "All RGCs")
  
  # Calculate Total Housing Units
  region_rgc <- p |> 
    group_by(estimate_year) |> 
    summarise(estimate = sum(housing_units)) |> 
    as_tibble() |>
    rename(year="estimate_year") |>
    mutate(center_boundary = rgc_title, name = "Region")
  
  hu_rgc <- bind_rows(rgc, rgc_total, region_rgc) |> mutate(name = factor(name, levels = rgc_names)) |> arrange(name)
  
  # Calculate Housing Units for MICs
  mic <- p |> 
    group_by(mic_name, estimate_year) |> 
    summarise(estimate = sum(housing_units)) |> 
    as_tibble() |>
    rename(year="estimate_year", name="mic_name") |>
    mutate(center_boundary = mic_title)
  
  # Calculate Housing Units for All MICs
  mic_total <- p |> 
    filter(mic_name != "Not in Center") |>
    group_by(estimate_year) |> 
    summarise(estimate = sum(housing_units)) |> 
    as_tibble() |>
    rename(year="estimate_year") |>
    mutate(center_boundary = mic_title, name = "All MICs")
  
  # Calculate Total Housing Units
  region_mic <- p |> 
    group_by(estimate_year) |> 
    summarise(estimate = sum(housing_units)) |> 
    as_tibble() |>
    rename(year="estimate_year") |>
    mutate(center_boundary = mic_title, name = "Region")
  
  hu_mic <- bind_rows(mic, mic_total, region_mic) |> mutate(name = factor(name, levels = mic_names)) |> arrange(name)
  
  if (is.null(ofm_housing_units)) {ofm_housing_units <- bind_rows(hu_rgc, hu_mic)} else {ofm_housing_units <- bind_rows(ofm_housing_units, hu_rgc, hu_mic)}
  
  rm(p, q, d, hu_rgc, rgc, rgc_total, region_rgc, hu_mic, mic, mic_total, region_mic)
  
}

print(str_glue("Final Cleanup for Housing Units data"))
ofm_housing_units <- ofm_housing_units  |> mutate(estimate = as.integer(estimate), metric = "Total Housing Units", grouping = "All")

