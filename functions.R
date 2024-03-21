centers_estimate_from_bg <- function(split_df, estimate_df, center_type, split_type, center_name) {
  
  # Filter Blockgroup Splits to Center Name
  t <- split_df |> 
    filter(planning_geog_type == center_type & planning_geog == center_name) |>
    select(year = "ofm_estimate_year", geography = "data_geog", name = "planning_geog", split_share = all_of(split_type)) 
  
  d <- estimate_df |> select("year","geography", "grouping", "metric", "estimate", "moe")
  
  # Proportion Estimates and MoE for each block group in center by split share and aggreagate to center
  c <- left_join(t, d, by=c("year", "geography"), relationship = "many-to-many") |>
    mutate(center_estimate = estimate*split_share, center_moe = moe*split_share) |>
    group_by(year, name, grouping, metric) |>
    summarise(estimate = round(sum(center_estimate),0), moe = round(moe_sum(moe=center_moe, estimate=center_estimate),0)) |>
    as_tibble() |>
    mutate(center_boundary = center_type)
  
  # Calculate Shares of Total Population, Households or Housing Units
  totals <- c |>
    filter(grouping == "Total") |>
    select("name", "year", "metric", total="estimate")
  
  c <- left_join(c, totals, by=c("name", "year", "metric")) |>
    mutate(share = estimate / total, share_moe = moe / total) |>
    select(-"total")
  
  return(c)
  
}

process_acs_data_for_centers <-function(variable_lookup = "census-variable-lookups", splits = blockgroup_splits, census_metric, split_variable) {
  
  # Determine Table Name, variables and labels from Variable Lookup
  print(str_glue("Loading the {census_metric} variable and label lookups."))
  lookup <- read_csv(paste0("input-data/",variable_lookup, ".csv"), show_col_types = FALSE) |> 
    filter(metric == census_metric) |> 
    mutate(variable = paste0(table, "_", str_pad(variable, width=3, side = 'left', pad="0")))
  census_tbl <- lookup |> select("table") |> distinct() |> pull()
  census_var <- lookup |> select("variable") |> distinct() |> pull()
  census_lbl <- lookup |> select("variable", "metric", "simple_label")
  census_ord <- lookup |> arrange(order) |> select("simple_label") |> distinct() |> pull()
  
  # Download Mode Share for Region
  print(str_glue("Downloading {census_metric} data from ACS table {census_tbl} for the Region."))
  r <- get_acs_recs(geography="county", table.names = census_tbl, years = c(pre_api_year, api_years), acs.type = 'acs5') |> filter(variable %in% census_var)
  
  # Clean Up labels and group data by labels
  print(str_glue("Cleaning up labels and grouping {census_metric} data from ACS table {census_tbl} for the Region."))
  r <- left_join(r, census_lbl, by=c("variable")) |>
    select("name", "estimate", "moe", "year", "metric", grouping="simple_label") |>
    mutate(name = paste(name, "Total")) |>
    group_by(name, year, metric, grouping) |>
    summarise(estimate = sum(estimate), moe = moe_sum(moe=moe, estimate=estimate)) |>
    as_tibble() |>
    mutate(grouping = factor(grouping, levels = census_ord)) |>
    mutate(name = factor(name, levels = county_names)) |>
    arrange(year, name, grouping)
  
  # Calculate Shares of Total Population, Households or Housing Units
  totals <- r |>
    filter(grouping == "Total") |>
    select("name", "year", "metric", total="estimate")
  
  r <- left_join(r, totals, by=c("name", "year", "metric")) |>
    mutate(share = estimate / total, share_moe = moe / total, center_boundary = "Region") |>
    select(-"total")
  
  # Download Blockgroup data from 2013 onward
  print(str_glue("Downloading Blockgroup {census_metric} data for {api_years} via the census api for ACS table {census_tbl}"))
  bg_api <- get_acs_recs(geography="block group", table.names = census_tbl, years = api_years, acs.type = 'acs5') |> filter(variable %in% census_var)
  
  # Clean Up labels and group data by labels
  print(str_glue("Cleaning up labels and grouping {census_metric} data from ACS table {census_tbl} for blockgroups for {api_years}."))
  bg_api <- left_join(bg_api, census_lbl, by=c("variable")) |>
    select(geography="GEOID", "estimate", "moe", "year", "metric", grouping="simple_label") |>
    group_by(geography, year, metric, grouping) |>
    summarise(estimate = sum(estimate), moe = moe_sum(moe=moe, estimate=estimate)) |>
    as_tibble() |>
    mutate(grouping = factor(grouping, levels = census_ord)) |>
    arrange(year, geography, grouping)
  
  # Pre-2013 Blockgroup data
  if(!(is.null(pre_api_year))) {
    
    bg_pre <- NULL
    for (y in pre_api_year) {
      
      print(str_glue("Working on Blockgroup {census_metric} data for {y} using stored files at {acs_pre2013_bg_dir}"))
      pre_file <- file.path(acs_pre2013_bg_dir, paste0("acs_",y,"5_", census_tbl, "_150.xlsx"))
      
      t <- read_excel(pre_file, sheet=paste0("ACS_",y,"5_", census_tbl, "_150"), skip=7) |>
        mutate(geoid = as.character(str_remove_all(`Geographic Identifier`, "15000US"))) |>
        select(-"Geographic Identifier",-"Table ID", -"Title", -"Universe", -"Summary Level Type", -"Summary Level", -"Area Name", -"ACS Dataset") |>
        pivot_longer(!geoid, names_to = "variable", values_to = "estimate") |>
        mutate(table = census_tbl) |>
        mutate(v = str_remove_all(variable, paste0(census_tbl,"E"))) |>
        mutate(v = str_remove_all(v, paste0(census_tbl,"M"))) |>
        mutate(metric_type = case_when(
          str_detect(variable, "E") ~ "estimate",
          str_detect(variable, "M") ~ "moe")) |>
        mutate(v = str_pad(v, width=3, side='left', pad="0")) |>
        mutate(variable = paste0(table,"_",v)) |>
        select(geography = "geoid", "variable", "metric_type", "estimate") |>
        pivot_wider(names_from = metric_type, values_from = estimate) |>
        mutate(year = y) |>
        filter(variable %in% census_var)
      
      t <- left_join(t, census_lbl, by=c("variable")) |>
        rename(grouping = "simple_label") |>
        group_by(geography, year, metric, grouping) |>
        summarise(estimate = sum(estimate), moe = moe_sum(moe=moe, estimate=estimate)) |>
        as_tibble() |>
        mutate(grouping = factor(grouping, levels = census_ord)) |>
        arrange(year, geography, grouping)
      
      if(is.null(bg_pre)) {bg_pre=t} else {bg_pre=bind_rows(bg_pre, t)}
      rm(t)
      
    } # end of loop over pre_api years
  } # end of pre-api year if statement
  
  # Combine Blockgroup Data
  print(str_glue("Combining the {c(pre_api_year, api_years)} blockgroup data for processing into centers"))
  blockgroups <- bind_rows(bg_pre, bg_api) |> arrange(year, geography, grouping)
  
  # Summarise Data by Center
  centers <- NULL
  for(center in rgc_names) {
    
    df <- centers_estimate_from_bg(estimate_df = blockgroups, split_df = splits, center_type = rgc_title, split_type = split_variable, center_name=center)
    ifelse(is.null(centers), centers <- df, centers <- bind_rows(centers, df))
    rm(df)
    
  }
  
  # Manufacturing and Industrial Centers
  for(center in mic_names) {
    
    df <- centers_estimate_from_bg(estimate_df = blockgroups, split_df = splits, center_type = mic_title, split_type = split_variable, center_name=center)
    centers <- bind_rows(centers, df)
    rm(df)
    
  }
  
  tbl <- bind_rows(centers, r)
  
  # Summarise Data by Urban Growth Area
  uga_data <- NULL
  for(uga in uga_names) {
    
    df <- centers_estimate_from_bg(estimate_df = blockgroups, split_df = splits, center_type = rgeo_title, split_type = split_variable, center_name=uga)
    ifelse(is.null(uga_data), uga_data <- df, uga_data <- bind_rows(uga_data, df))
    rm(df)
    
  }
  
  tbl <- bind_rows(tbl, uga_data) |> mutate(name = factor(name, levels = all_names)) |> arrange(year, name, grouping)
  
  return(tbl)
  
}

create_summary_spreadsheet <- function(summary_data, acs_metric_ids) {
  
  # Basics of output Spreadsheet
  hs <- createStyle(
    fontColour = "black",
    border = "bottom",
    fgFill = "#00a7a0",
    halign = "center",
    valign = "center",
    textDecoration = "bold"
  )
  
  ns <- createStyle(
    fontName = "Poppins",
    fontColour = "black",
    fontSize = 11,
    halign = "left",
    valign = "center",
  )
  
  table_idx <- 1
  sheet_idx <- 2
  
  wb <- loadWorkbook("input-data/metadata.xlsx")
  analysis_years <- paste(pre_api_year, api_years, sep = ",")
  
  # Set Font Style
  addStyle(wb = wb, sheet = "Data Notes", style = ns, rows = 1:2, cols = 2)
  
  writeData(
    wb = wb,
    sheet = "Data Notes",
    x = analysis_years,
    xy = c(2,1))
  
  writeData(
    wb = wb,
    sheet = "Data Notes",
    x = Sys.Date(),
    xy = c(2,2))
  
  for (i in acs_metric_ids) {
    
    tbl <- summary_data
    
    addWorksheet(wb, sheetName = i)
    writeDataTable(wb, sheet = sheet_idx, x = tbl, tableStyle = "none", headerStyle = hs, withFilter = FALSE)
    setColWidths(wb, sheet = sheet_idx, cols = 1:length(tbl), widths = "auto")
    freezePane(wb, sheet = sheet_idx, firstRow = TRUE)
    
    if (table_idx < length(acs_metric_ids)) {
      table_idx <- table_idx + 1
      sheet_idx <- sheet_idx + 1
      
    } else {break}
    
  }
  
  saveWorkbook(wb, file = paste0("output-data/acs_data_for_centers_monitoring_metric_", acs_metric_ids, ".xlsx"), overwrite = TRUE)
}

summary_table_creation <- function(census_metric, census_data=centers_acs_data) {
  
  print(str_glue("Creating Cleaned table for {census_metric} for final output"))
  tbl <- census_data |> filter(metric == census_metric & grouping != "Total") |> select(-"metric", -"center_boundary")
  output_id <- read_csv("input-data/acs-centers-output-crosswalk.csv", show_col_types = FALSE) |> filter(metric == census_metric) |> select("output_id") |> pull()
  
  # Get unique values to use to create table layout
  yrs <- unique(tbl$year)
  vars <- unique(tbl$grouping)
  
  final_tbl <- tbl |> select("name") |> distinct() |> arrange(name)
  for (y in yrs) {
    for (v in vars) {
      t <- tbl |>
        filter(year == y & grouping == v) |>
        pivot_wider(names_from = grouping, 
                    values_from = c(estimate, moe, share, share_moe),
                    names_sep = ": ",
                    names_sort = FALSE,
                    names_prefix = paste0(as.character(y)," ")) |>
        select(-"year") 
      
      final_tbl <- left_join(final_tbl, t, by="name") 
      
    }
  }
  
  # Create Spreadsheet
  create_summary_spreadsheet(summary_data = final_tbl, acs_metric_ids = output_id)
}

generate_blockgroup_splits <- function(y) {
  
  if (y >=2020) {
    ofm_vin <- y
    geog_yr <- 'blockgroup20'
    
  } else {
    ofm_vin <- 2020
    geog_yr <- 'blockgroup10'}
  
  if (y >=2018) {parcel_yr <- 2018} else {parcel_yr <- 2014}
  
  # Regional Growth Centers 
  print(str_glue("Getting Blockgroup splits from Elmer for {geog_yr} for {rgc_title} for the year {y}"))
  q <- paste0("SELECT * FROM general.get_geography_splits('", geog_yr , "', '", rgc_title, "', ", y, ", ", ofm_vin,", ", parcel_yr,")")
  
  rgc_indvidual <- get_query(sql = q, db_name = "Elmer") |> 
    filter(planning_geog != "not in regional growth center") |> 
    mutate(planning_geog = str_replace_all(planning_geog, "Greater Downtown Kirkland", "Kirkland Greater Downtown"))
  
  rgc_all <- rgc_indvidual |> mutate(planning_geog = "Regional Growth Centers")
  
  rgc_county <- rgc_indvidual |> mutate(planning_geog = case_when(
    (str_detect(data_geog, "53033")) ~ "King County RGCs",
    (str_detect(data_geog, "53035")) ~ "Kitsap County RGCs",
    (str_detect(data_geog, "53053")) ~ "Pierce County RGCs",
    (str_detect(data_geog, "53061")) ~ "Snohomish County RGCs"))
  
  rgc_type <- rgc_indvidual |> mutate(planning_geog = case_when(
    (planning_geog %in% rgc_metro) ~ "Metro Growth Centers",
    (planning_geog %in% rgc_urban) ~ "Urban Growth Centers"))  
  
  rgc_not <- get_query(sql = q, db_name = "Elmer") |> 
    filter(planning_geog == "not in regional growth center") |> 
    mutate(planning_geog = "Not in a Regional Growth Center")
  
  # Manufacturing Industrial Centers
  print(str_glue("Getting Blockgroup splits from Elmer for {geog_yr} for {mic_title} for the year {y}"))
  q <- paste0("SELECT * FROM general.get_geography_splits('", geog_yr , "', '", mic_title, "', ", y, ", ", ofm_vin,", ", parcel_yr,")")
  
  mic_indvidual <- get_query(sql = q, db_name = "Elmer") |> 
    filter(planning_geog != "not in MIC") |>
    mutate(planning_geog = str_replace_all(planning_geog, "Puget Sound Industrial Center- Bremerton", "Puget Sound Industrial Center - Bremerton"))
  
  mic_all <- mic_indvidual |> mutate(planning_geog = "Manufacturing Industrial Centers")
  
  mic_county <- mic_indvidual |> mutate(planning_geog = case_when(
    (str_detect(data_geog, "53033")) ~ "King County MICs",
    (str_detect(data_geog, "53035")) ~ "Kitsap County MICs",
    (str_detect(data_geog, "53053")) ~ "Pierce County MICs",
    (str_detect(data_geog, "53061")) ~ "Snohomish County MICs"))
  
  mic_type <- mic_indvidual |> mutate(planning_geog = case_when(
    (planning_geog %in% mic_employment) ~ "Industrial Employment Centers",
    (planning_geog %in% mic_growth) ~ "Industrial Growth Centers")) 
  
  mic_not <- get_query(sql = q, db_name = "Elmer") |> 
    filter(planning_geog == "not in MIC") |> 
    mutate(planning_geog = "Not in a Manufacturing Industrial Centers")
  
  # Urban Growth Area
  print(str_glue("Getting Blockgroup splits from Elmer for {geog_yr} for {rgeo_title} for the year {y}"))
  q <- paste0("SELECT * FROM general.get_geography_splits('", geog_yr , "', '", rgeo_title, "', ", y, ", ", ofm_vin,", ", parcel_yr,")")
  
  uga_all <- get_query(sql = q, db_name = "Elmer") |> filter(planning_geog != "Rural") |> mutate(planning_geog = "UGA Total")
  
  uga_county <- uga_all |>
    mutate(planning_geog = case_when(
      (str_detect(data_geog, "53033")) ~ "King County UGA",
      (str_detect(data_geog, "53035")) ~ "Kitsap County UGA",
      (str_detect(data_geog, "53053")) ~ "Pierce County UGA",
      (str_detect(data_geog, "53061")) ~ "Snohomish County UGA"))
  
  splits <- bind_rows(rgc_indvidual, rgc_all, rgc_county, rgc_type, rgc_not,
                      mic_indvidual, mic_all, mic_county, mic_type, mic_not, 
                      uga_all, uga_county)
  
  return(splits)
  
}


