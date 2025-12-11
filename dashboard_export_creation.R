#!/usr/bin/env Rscript

## ---- ProACT ---- ##
## First time edited: 12/11/2025, by Dani
## Last time edited: 12/11/2025, by Dani

suppressPackageStartupMessages({
  library(optparse)
  library(tidyverse)
  library(lubridate)
  library(data.table)
})

# Load ISO matcher for ISO3 codes
# iso_matcher <- fread("/gti/tmp/_UNDP/Utility_datasets/Other/ISO_matcher.csv") # Server PATH
iso_matcher <- fread("C:/GTI/TMP/ISO_matcher.csv") # Dani local path
setnames(iso_matcher, tolower(names(iso_matcher)))

# ---------- CLI ----------
opt_list <- list(
  make_option(c("--input-dir"), type="character", default="C:/GTI/ProACT 2025/Data/Input_data"),
  make_option(c("--output-dir"), type="character", default="C:/GTI/ProACT 2025/Data/Output_data"),
  make_option(c("--output-subdir"), type="character", default=format(Sys.Date(), "%m%d")),
  make_option(c("--min-contracts"), type="integer", default=10),
  make_option(c("--verbose"), action="store_true", default=FALSE)
)
opt <- parse_args(OptionParser(option_list = opt_list))

vcat <- function(...) {
  if (isTRUE(opt$verbose)) {
    timestamp <- format(Sys.time(), "%H:%M:%S")
    cat(sprintf("[%s - INFO][ProACT_exports]", timestamp), sprintf(...), "\n")
  }
}

# ---------- I/O ----------
out_dir <- file.path(opt$`output-dir`, opt$`output-subdir`)
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
vcat("Output: %s", out_dir)

# ---------- Updated indicators list ----------
list_of_indicators <- c(
  "ind_tr_proc_type",
  "ind_tr_buyer_id",
  "ind_tr_supplier_id",
  "ind_tr_bidder_id",
  "ind_tr_call_pub",
  "ind_tr_bid_deadline",
  "ind_tr_bid_opening",
  "ind_tr_award_pub",
  "ind_tr_prod_code",
  "ind_tr_buyer_loc",
  "ind_tr_supplier_loc",
  "ind_tr_impl_loc",
  "ind_tr_bidder_loc",
  "ind_tr_contract_value",
  "ind_tr_benford",
  "ind_op_open_proc",
  "ind_op_nonopen_proc",
  "ind_op_adv_period",
  "ind_op_short_adv_flag",
  "ind_adm_dec_period",
  "ind_adm_long_dec_flag",
  "ind_comp_avg_bids",
  "ind_comp_single_bid",
  "ind_comp_new_suppliers",
  "ind_comp_sector_concentration",
  "ind_comp_foreign_suppliers",
  "ind_comp_tax_haven_suppliers",
  "ind_econ_sector_composition",
  "ind_econ_buyer_concentration",
  "ind_impl_cost_overrun",
  "ind_impl_time_overrun"
)

## ProACT Dashboard Export Function ####

# Modified function to calculate aggregate values with HIGH/MED/LOW tiers
calculate_proact_aggregates <- function(dt, indicator) {
  # Ensure dt is a data.table and sort in descending order by bid_price
  if (!is.data.table(dt)) {
    dt <- as.data.table(dt)
  }
  setorder(dt, -bid_price)

  # Helper function to calculate statistics for a subset
  calc_stats <- function(subset_dt) {
    if (nrow(subset_dt) == 0) {
      return(list(
        mean_val = NA_real_,
        numerator = NA_integer_,
        denominator = NA_integer_,
        n_obs = 0L,
        total_value = 0
      ))
    }

    list(
      mean_val = mean(subset_dt[[indicator]], na.rm = TRUE),
      numerator = sum(subset_dt[[indicator]] == 1, na.rm = TRUE),
      denominator = sum(!is.na(subset_dt[[indicator]])),
      n_obs = nrow(subset_dt),
      total_value = sum(subset_dt$bid_price, na.rm = TRUE) / 1000000
    )
  }

  # Calculate for HIGH contracts (>= 5,000,000)
  high_stats <- calc_stats(dt[bid_price >= 5000000])

  # Calculate for MED contracts (>= 500,000 and < 5,000,000)
  med_stats <- calc_stats(dt[bid_price >= 500000 & bid_price < 5000000])

  # Calculate for LOW contracts (< 500,000)
  low_stats <- calc_stats(dt[bid_price < 500000])

  # Return results as data.table
  data.table(
    Indicator = indicator,
    Contract_value = c("HIGH", "MED", "LOW"),
    Indicator_value = c(high_stats$mean_val, med_stats$mean_val, low_stats$mean_val),
    Total_number_of_risky_contracts = c(high_stats$numerator, med_stats$numerator, low_stats$numerator),
    All_contracts = c(high_stats$denominator, med_stats$denominator, low_stats$denominator),
    n_observations = c(high_stats$n_obs, med_stats$n_obs, low_stats$n_obs),
    Total_contract_value_million_usd = round(c(high_stats$total_value, med_stats$total_value, low_stats$total_value), 2)
  )
}

# III. Load and process files ####

files_to_load <- list.files(opt$`input-dir`, pattern="_export\\.csv$", full.names=TRUE, recursive = TRUE)
stopifnot(length(files_to_load) > 0)

# Select most recent file per country
file_info <- data.table(
  filepath = files_to_load,
  filename = basename(files_to_load)
)

# Extract country code (first 2 chars)
file_info[, country_code := substr(filename, 1, 2)]

# If there are date stamps in filenames, keep most recent; otherwise keep all
if (any(grepl("\\d{8}", file_info$filename))) {
  file_info[, date_str := sub(".*_(\\d{8})_.*", "\\1", filename)]
  file_info <- file_info[order(-date_str)]
  file_info <- file_info[, .SD[1], by = country_code]
}

# Sort alphabetically by country code
file_info <- file_info[order(country_code)]
files_to_load <- file_info$filepath

vcat("Selected %d files", length(files_to_load))

## Process all countries ####

proact_export_all <- list()

for (file_path in files_to_load) {
  # Extract country code from filename
  country_code <- substr(basename(file_path), 1, 2)
  
  vcat("Processing %s: %s", country_code, basename(file_path))
  
  # Load CSV file
  df <- fread(file_path)
  
  # Filter years between 2017 and 2024
  df <- df[tender_year >= 2017 & tender_year <= 2024]
  
  vcat("  Year distribution: %s", paste(names(table(df$tender_year)), collapse=", "))
  
  # Fix integer overflow by converting to numeric
  if (is.integer(df$bid_price)) {
    vcat("  Converting bid_price to numeric for country %s", country_code)
    df[, bid_price := as.numeric(bid_price)]
  }
  
  # Add Country_code column if not present
  if (!"Country_code" %in% names(df)) {
    df[, Country_code := country_code]
  }
  
  # Calculate ProACT aggregates grouped by Country, tender_year, product_market_short_name
  proact_export <- df[, {
    rbindlist(lapply(list_of_indicators, function(ind) {
      calculate_proact_aggregates(.SD, ind)
    }))
  }, by = .(Country, Country_code, tender_year, product_market_short_name)]
  
  # Set values to NA for groups with less than min-contracts
  proact_export[n_observations < opt$`min-contracts`, `:=`(
    Indicator_value = NA_real_,
    Total_number_of_risky_contracts = NA_integer_,
    All_contracts = NA_integer_
  )]
  
  # Remove n_observations column
  proact_export[, n_observations := NULL]
  
  # Add Indicator_availability_filter column
  proact_export[, Indicator_availability_filter := fifelse(is.na(Indicator_value), 0L, 1L)]
  
  proact_export_all[[country_code]] <- proact_export
  
  vcat("  Completed %s", country_code)
}

# IV. Combine and export ####

# Combine all countries
vcat("Combining all country tables...")
proact_combined <- rbindlist(proact_export_all)

# Function to add ISO3 codes
add_iso3 <- function(dt, iso_matcher) {
  if (!is.data.table(dt)) {
    setDT(dt)
  }
  
  # Merge with iso matcher
  dt <- merge(dt, iso_matcher[, .(iso2, iso3)], 
              by.x = "Country_code", by.y = "iso2", 
              all.x = TRUE, sort = FALSE)
  
  # Custom mapping: EU -> EUE
  dt[Country_code == "EU", iso3 := "EUE"]
  
  # Check for unmatched countries
  unmatched <- dt[is.na(iso3), unique(Country_code)]
  if (length(unmatched) > 0) {
    warning(sprintf("ISO3 matching: No match found for Country_code(s): %s", 
                    paste(unmatched, collapse = ", ")))
    vcat("WARNING: ISO3 matching failed for: %s", paste(unmatched, collapse = ", "))
  }
  
  # Rename columns
  setnames(dt, "Country_code", "Country_code_ISO_2")
  setnames(dt, "iso3", "Country_code_ISO_3")
  
  return(dt)
}

# Add ISO3 codes
vcat("Adding ISO3 codes...")
proact_combined <- add_iso3(proact_combined, iso_matcher)

# Reorder columns: Country, ISO2, ISO3, then the rest
setcolorder(proact_combined, c(
  "Country", "Country_code_ISO_2", "Country_code_ISO_3",
  setdiff(names(proact_combined), c("Country", "Country_code_ISO_2", "Country_code_ISO_3"))
))

# Replace NaN with NA
proact_combined[proact_combined == "NaN"] <- NA

# Export combined table
output_file <- file.path(out_dir, "ProACT_dashboard_export.csv")
vcat("Exporting to: %s", output_file)
fwrite(proact_combined, output_file)

vcat("Export complete!")