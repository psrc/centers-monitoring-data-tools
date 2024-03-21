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

create_summary_spreadsheet <- function(min_yr_data, max_yr_data, acs_metric_ids, census_metric) {
  
  # Get Census Table 
  census_tbl <- read_csv("input-data/census-variable-lookups.csv", show_col_types = FALSE) |> filter(metric == census_metric) |> select("table") |> distinct() |> pull()
  
  # Determine Years
  min_yr <- unique(min_yr_data$year)
  max_yr <- unique(max_yr_data$year)
  yrs <- c(min_yr, max_yr)
  
  # Number of Rows
  num_rows <- min_yr_data |> select("name") |> pull() |> length()
  num_cols <- min_yr_data |> filter(name == "Regional Growth Centers") |> select(-"year") |> pivot_longer(!name, names_to = "variables") |> select("variables") |> pull() |> length()+1
  
  # Determine the Number of Columns of Output
  vars <- min_yr_data |>
    select(-"year") |>
    pivot_longer(!name, names_to = "variables") |>
    select("variables") |>
    mutate(variables = str_remove_all(variables, "estimate: ")) |>
    mutate(variables = str_remove_all(variables, "share_moe: ")) |>
    mutate(variables = str_remove_all(variables, "moe: ")) |>
    mutate(variables = str_remove_all(variables, "share: ")) |>
    distinct() |>
    pull()
  
  num_vars <- length(vars)
  
  title_row_2 <- c("Geography", rep(c("Estimate", "MoE", "Share", "Share MoE"), num_vars))
  
  # List of Columns containing numbers
  estimate_columns <- c(2)
  moe_columns <- c(3)
  for(n in 1:(num_vars-1)) {
    estimate_columns<- append(estimate_columns,2+(4*n))
    moe_columns<- append(moe_columns,3+(4*n))
  }
  
  numeric_columns<- c(estimate_columns, moe_columns)
  
  # List of Columns containing percentages
  estimate_columns <- c(4)
  moe_columns <- c(5)
  for(n in 1:(num_vars-1)) {
    estimate_columns<- append(estimate_columns,4+(4*n))
    moe_columns<- append(moe_columns,5+(4*n))
  }
  
  percent_columns<- c(estimate_columns, moe_columns)
  
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
  
  # Styles for Center Names
  rs <- createStyle(textDecoration = "bold", indent = 0)
  cos <- createStyle(indent = 1)
  cns <- createStyle(indent = 2)
  
  # Styles for Columns
  pct_fmt <- createStyle(numFmt = "0.0%", halign = "center")
  num_fmt <- createStyle(numFmt = "#,##0", halign = "center")
  centerStyle <- createStyle(halign = "center")
  
  table_idx <- 1
  sheet_idx <- 2
  
  wb <- loadWorkbook("input-data/metadata.xlsx")
  analysis_years <- paste(pre_api_year, api_years, sep = ",")
  
  # Set Font Style
  addStyle(wb = wb, sheet = "Notes", style = ns, rows = 1:4, cols = 2)
  
  writeData(
    wb = wb,
    sheet = "Notes",
    x = analysis_years,
    xy = c(2,1))
  
  writeData(
    wb = wb,
    sheet = "Notes",
    x = Sys.Date(),
    xy = c(2,2))
  
  writeData(
    wb = wb,
    sheet = "Notes",
    x = census_metric,
    xy = c(2,3))
  
  writeData(
    wb = wb,
    sheet = "Notes",
    x = census_tbl,
    xy = c(2,4))
  
  for (i in yrs) {
    
    ifelse(i == min_yr, tbl <- min_yr_data, tbl <- max_yr_data)
    
    tbl <- tbl |> select(-"year")
    
    # Write the Variable Names in Row # 1
    writeData(
      wb = wb,
      sheet = as.character(i),
      x = "Geography",
      xy = c(1,1))
    
    l = 0
    for(n in vars) {
      
      writeData(
        wb = wb,
        sheet = as.character(i),
        x = n,
        xy = c(2+l,1))
      
      s <- 2+l
      e <- s + 3
      mergeCells(wb, sheet = as.character(i), rows = 1, cols = s:e)
      addStyle(wb, sheet = as.character(i), centerStyle, rows = 1, cols = s:e)
      
      l <- l + 4
      
    }
    
    # Write the Data Headings (Estimate, MoE, Share, Share MoE)
    for(j in 1:length(title_row_2)) {
      
      writeData(
        wb = wb,
        sheet = as.character(i),
        x = title_row_2[[j]],
        xy = c(j,2))
      
    }
    
    # Write data without the column headings
    writeData(
      wb = wb,
      sheet = as.character(i),
      x = tbl,
      colNames = FALSE,
      xy = c(1,3))
    
    # Merge the Geography Title
    mergeCells(wb, sheet = as.character(i), rows = 1:2, cols = 1)
    
    # Set width for Column #1 to 45
    setColWidths(wb, sheet = as.character(i), cols = 1:1, widths = 45)
    
    # Set all other column widths to 10
    setColWidths(wb, sheet = as.character(i), cols = 2:length(tbl), widths = 10)
    
    # Center Data columns, not the first column
    for(r in 1:(num_rows+2)) {
      addStyle(wb, sheet = as.character(i), style=centerStyle, rows = r, cols = 2:num_cols)
    }
    
    # Make Estimate and MoE columns numeric with 1000s seperator
    for(r in 3:(num_rows+2)) {
      
      for(c in numeric_columns) {
        addStyle(wb, sheet = as.character(i), style=num_fmt, rows = r, cols = c)
      }
    }
    
    # Make Share and Share MoE columns percentages with 1 decimal place
    for(r in 3:(num_rows+2)) {
      
      for(c in percent_columns) {
        addStyle(wb, sheet = as.character(i), style=pct_fmt, rows = r, cols = c)
      }
    }
    
    # Freeze Panes starting with Row 3
    freezePane(wb, sheet = as.character(i), firstActiveRow = 3)
    
    # RGC Region Headings
    addStyle(wb, sheet = as.character(i), style=rs, rows = 3, cols = 1)
    
    # RGC King County Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 4, cols = 1)
    
    # RGC King County Center Headings
    addStyle(wb, sheet = as.character(i), style=cns, rows = 5:23, cols = 1)
    
    # RGC Kitsap County Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 24, cols = 1)
    
    # RGC Kitsap County Center Headings
    addStyle(wb, sheet = as.character(i), style=cns, rows = 25:26, cols = 1)
    
    # RGC Pierce County Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 27, cols = 1)
    
    # RGC Pierce County Center Headings
    addStyle(wb, sheet = as.character(i), style=cns, rows = 28:33, cols = 1)
    
    # RGC Snohomish County Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 34, cols = 1)
    
    # RGC Snohomomish County Center Headings
    addStyle(wb, sheet = as.character(i), style=cns, rows = 35:37, cols = 1)
    
    # RGC Type Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 38:40, cols = 1)
    
    # MIC Region Headings
    addStyle(wb, sheet = as.character(i), style=rs, rows = 41, cols = 1)
    
    # MIC King County Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 42, cols = 1)
    
    # MIC King County Center Headings
    addStyle(wb, sheet = as.character(i), style=cns, rows = 43:46, cols = 1)
    
    # MIC Kitsap County Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 47, cols = 1)
    
    # MIC Kitsap County Center Headings
    addStyle(wb, sheet = as.character(i), style=cns, rows = 48, cols = 1)
    
    # MIC Pierce County Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 49, cols = 1)
    
    # MIC Pierce County Center Headings
    addStyle(wb, sheet = as.character(i), style=cns, rows = 50:52, cols = 1)
    
    # MIC Snohomish County Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 53, cols = 1)
    
    # MIC Snohomomish County Center Headings
    addStyle(wb, sheet = as.character(i), style=cns, rows = 54:55, cols = 1)
    
    # MIC Type Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 56:58, cols = 1)
    
    # Region Headings
    addStyle(wb, sheet = as.character(i), style=rs, rows = 59, cols = 1)
    
    # Region County Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 60:63, cols = 1)
    
    # UGA Headings
    addStyle(wb, sheet = as.character(i), style=rs, rows = 64, cols = 1)
    
    # UGA County Headings
    addStyle(wb, sheet = as.character(i), style=cos, rows = 65:68, cols = 1)
    

  }
  
  saveWorkbook(wb, file = paste0("output-data/metric_", acs_metric_ids, ".xlsx"), overwrite = TRUE)
}

summary_table_creation <- function(census_metric, census_data=centers_acs_data) {
  
  print(str_glue("Creating Cleaned table for {census_metric} for final output"))
  tbl <- census_data |> filter(metric == census_metric) |> select(-"metric", -"center_boundary")
  output_id <- read_csv("input-data/acs-centers-output-crosswalk.csv", show_col_types = FALSE) |> filter(metric == census_metric) |> select("output_id") |> pull()
  
  # Get unique values to use to create table layout
  yrs <- unique(tbl$year)
  vars <- unique(tbl$grouping)
  
  min_yr <- min(yrs)
  max_yr <- max(yrs)
  yr1 <- tbl |> select("name") |> mutate(year = min_yr) |> distinct() |> arrange(name)
  yr2 <- tbl |> select("name") |> mutate(year = max_yr) |>distinct() |> arrange(name)
  for (v in vars) {
    # Minimum Year
    t <- tbl |>
      filter(year == min_yr & grouping == v) |>
      pivot_wider(names_from = grouping, 
                  values_from = c(estimate, moe, share, share_moe),
                  names_sep = ": ",
                  names_sort = FALSE)
    
    yr1 <- left_join(yr1, t, by=c("name", "year")) 
    
    # Maximum Year
    t <- tbl |>
      filter(year == max_yr & grouping == v) |>
      pivot_wider(names_from = grouping, 
                  values_from = c(estimate, moe, share, share_moe),
                  names_sep = ": ",
                  names_sort = FALSE)
    
    yr2 <- left_join(yr2, t, by=c("name", "year"))
  }
  
  # Create Spreadsheet
  create_summary_spreadsheet(min_yr_data = yr1, max_yr_data = yr2, acs_metric_ids = output_id, census_metric = census_metric)
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


