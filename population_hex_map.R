# Libraries -----------------------------------------------------------------
library(tidyverse)
library(psrcelmer)
library(sf)

# Silence the dplyr summarize message
options(dplyr.summarise.inform = FALSE)

# Population Years from OFM Parcelization
population_years <- c(2010, 2023)

# Coordinate Reference System
spn <- 2285

create_ofm_grid_data <- function(y, g=2) {
  
  # Parcel population
  if (y >= 2020) {ofm_vintage <- y} else {ofm_vintage <- 2020}
  print(str_glue("Loading OFM based parcelized estimates of total population for {y}"))
  q <- paste0("SELECT parcel_dim_id, estimate_year, total_pop FROM ofm.parcelized_saep_facts WHERE ofm_vintage = ", ofm_vintage, " AND estimate_year = ", y, "")
  p <- get_query(sql = q)
  
  # Parcel Dimensions
  if (y >=2018) {parcel_yr <- 2018} else {parcel_yr <- 2014}
  print(str_glue("Loading {parcel_yr} parcel dimensions from Elmer"))
  q <- paste0("SELECT parcel_dim_id, parcel_id, x_coord_state_plane, y_coord_state_plane from small_areas.parcel_dim WHERE base_year = ", parcel_yr, " ")
  d <- get_query(sql = q)
  
  # Add Dimensions to the Parcel Population Data
  print(str_glue("Adding parcel x and y coordinates to parcel population for {y}"))
  p <- left_join(p, d, by="parcel_dim_id") |> select("parcel_id", x="x_coord_state_plane", y="y_coord_state_plane", population="total_pop", year="estimate_year")
  
  # Convert Parcels to a SF object
  print(str_glue("Converting {y} parcel data to a spatial object"))
  p_layer <- st_as_sf(x=p, coords = c("x", "y"), crs = spn)
  
  # Convert Parcel data to a hexagonal grid
  print(str_glue("Converting {y} parcel data to a hecagonal grid size of {g*5280} feet"))
  p_grid <- st_make_grid(p_layer, cellsize = ((g*5280)^2)/sqrt(3), square = FALSE) |> st_sf()
    
    
  
  #st_write(p_grid, "output-data/test.shp", append=FALSE)
  
}


