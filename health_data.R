# Libraries -----------------------------------------------------------------
library(tidyverse)
library(psrccensus)
library(psrcelmer)
library(tidycensus)
library(sf)

source("functions.R")

# Basic Inputs ------------------------------------------------------------

# Silence the dplyr summarize message
options(dplyr.summarise.inform = FALSE)

# Make sure these match the boundary definition you want the data to be based on
rgc_title <- "Regional Growth Center (4/23/2024)"
mic_title <- "MIC (4/23/2024)"
rgeo_title <- "Regional Geography Class (2022 RTP)"

health_years <- c(2010, 2015, 2020)

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

# Tract Splits ------------------------------------------------------------
tract_splits <- NULL
for (yr in health_years) {
  splits <- generate_tract_splits(y = yr)
  if (is.null(tract_splits)) {tract_splits <- splits} else {tract_splits <- bind_rows(tract_splits, splits)}
  rm(splits)
}

# Life Expectancy from WTN ----------------------------------------------------
folder_path <- "Y:/Equity Indicators/tracker-webpage-content/a-regional-health-collaboration/a01-life-expectancy/raw-data"

life_expectancy <- NULL
for (y in health_years) {
  
  t <- read_csv(paste0(folder_path,"/Life_Expectancy_at_Birth_", str_pad((y-2000) - 4, width = 2, pad = 0), "_", y-2000, ".csv")) |>
    filter(`County Name` %in% c("King", "Kitsap", "Pierce", "Snohomish")) |>
    mutate(`Life Expectancy` = as.numeric(`Life Expectancy`)) |>
    mutate(`Lower CI` = as.numeric(`Lower CI`)) |>
    mutate(`Upper CI` = as.numeric(`Upper CI`)) |>
    mutate(year = y) |>
    select(geoid10 = "Census Tract", life_expectancy = "Life Expectancy", lower_ci = "Lower CI", upper_ci = "Upper CI", "year")
  
  # Since WTN is in 2010 tracts, convert to 2020 tracts if the year is 2020 or later otherwise keep 2010 tract geography
  if (y >= 2020) {
    tracts <- get_table(schema="census", tbl_name="v_geo_relationships_tracts") |> select(-"geog_type") |> rename(geoid="geoid20")
    tracts <- left_join(tracts, t, by=c("geoid10")) |> select(-"geoid10")
    
  } else {
    
    tracts <- t |> rename(geoid = "geoid10")
    
  }
  
  if(is.null(life_expectancy)) {life_expectancy <- tracts} else {life_expectancy <- bind_rows(life_expectancy, tracts)}
  rm(t, tracts)
  
}

# Population Data by Census Tract 
tract_population <- get_acs_recs(geography ='tract', table.names = 'B01003', years = health_years) |>
  filter(variable %in% c("B01003_001")) |>
  select(geoid = "GEOID", population = "estimate", "year")

# Join Population data by Tract with Life Expectancy Data
tract_data <- left_join(life_expectancy, tract_population,  by=c("geoid", "year")) |> arrange(year, geoid)
rm(life_expectancy, tract_population)

centers_life_expectancy <- process_life_expectancy_data_for_centers(estimate_df=tract_data, wtn_metric="life_expectancy")
write_csv(centers_life_expectancy, "output-data/metric_39.csv")

# Cardiovascular Disease from WTN ----------------------------------------------------
folder_path <- "Y:/Equity Indicators/tracker-webpage-content/a-regional-health-collaboration/a02-cardiovascular-disease-mortality/raw-data"

cardiovascular_disease <- NULL
for (y in health_years) {
  
  t <- read_csv(paste0(folder_path,"/Cardiovascular_Disease_Mortality_", str_pad((y-2000) - 4, width = 2, pad = 0), "_", y-2000, ".csv")) |>
    filter(`County Name` %in% c("King", "Kitsap", "Pierce", "Snohomish")) |>
    mutate(`Age-Adjusted Rate per 100,000` = as.numeric(`Age-Adjusted Rate per 100,000`)) |>
    mutate(`Lower CI` = as.numeric(`Lower CI`)) |>
    mutate(`Upper CI` = as.numeric(`Upper CI`)) |>
    mutate(year = y) |>
    select(geoid10 = "Census Tract", cardiovascular_rate = "Age-Adjusted Rate per 100,000", lower_ci = "Lower CI", upper_ci = "Upper CI", "year")
  
  # Since WTN is in 2010 tracts, convert to 2020 tracts if the year is 2020 or later otherwise keep 2010 tract geography
  if (y >= 2020) {
    tracts <- get_table(schema="census", tbl_name="v_geo_relationships_tracts") |> select(-"geog_type") |> rename(geoid="geoid20")
    tracts <- left_join(tracts, t, by=c("geoid10")) |> select(-"geoid10")
    
  } else {
    
    tracts <- t |> rename(geoid = "geoid10")
    
  }
  
  if(is.null(cardiovascular_disease)) {cardiovascular_disease <- tracts} else {cardiovascular_disease <- bind_rows(cardiovascular_disease, tracts)}
  rm(t, tracts)
  
}

# Population Data by Census Tract 
tract_population <- get_acs_recs(geography ='tract', table.names = 'B01003', years = health_years) |>
  filter(variable %in% c("B01003_001")) |>
  select(geoid = "GEOID", population = "estimate", "year")

# Join Population data by Tract with Cardiovascular Data
tract_data <- left_join(cardiovascular_disease, tract_population,  by=c("geoid", "year")) |> 
  mutate(name = case_when(
    str_detect(geoid, "53033") ~ "King County Total",
    str_detect(geoid, "53035") ~ "Kitsap County Total",
    str_detect(geoid, "53053") ~ "Pierce County Total",
    str_detect(geoid, "53061") ~ "Snohomish County Total")) |>
  mutate(cardiovascular_estimate = cardiovascular_rate * (population/100000)) |>
  mutate(cardiovascular_lower = lower_ci * (population/100000)) |>
  mutate(cardiovascular_upper = upper_ci * (population/100000)) |>
  arrange(year, geoid)

rm(cardiovascular_disease, tract_population)

centers_cardiovascular <- process_cardiovascular_data_for_centers(estimate_df=tract_data)
write_csv(centers_cardiovascular, "output-data/metric_40.csv")
