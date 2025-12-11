## ---- ProACT Mock Indicator Generator ---- ##
## First time edited: 12/11/2025, by Dani
## Last time edited: 12/11/2025, by Dani

suppressPackageStartupMessages({
  library(tidyverse)
  library(data.table)
  library(readxl)
})

# Set paths
input_folder <- "C:/GTI/ProACT 2025/Data/Input_data"
output_folder <- "C:/GTI/ProACT 2025/Data/Input_data"  # Same folder for outputs
iso_matcher_path <- "C:/GTI/TMP/ISO_matcher.csv"
correspondence_table_path <- "C:/GTI/TMP/Correspondence_table_UNDP2025.xlsx"

# Load ISO matcher
iso_matcher <- fread(iso_matcher_path)
setnames(iso_matcher, tolower(names(iso_matcher)))

# Load correspondence table
cpv_labels <- read_excel(correspondence_table_path, sheet = "cpv_labels")
cpv_labels <- as.data.table(cpv_labels)

# List of ProACT indicators
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

# Get all CSV export files
files_to_process <- list.files(input_folder, pattern = "_export\\.csv$", full.names = TRUE)

if (length(files_to_process) == 0) {
  stop("No files matching pattern '_export.csv' found in input folder")
}

cat(sprintf("Found %d files to process\n", length(files_to_process)))

# Process each file
for (file_path in files_to_process) {
  filename <- basename(file_path)
  country_code <- substr(filename, 1, 2)
  
  cat(sprintf("\nProcessing: %s (Country: %s)\n", filename, country_code))
  
  # Read the file
  df <- fread(file_path)
  
  # Get country name from ISO matcher
  country_name <- iso_matcher[iso2 == country_code, country]
  if (length(country_name) == 0) {
    warning(sprintf("Country code %s not found in ISO matcher. Using code as name.", country_code))
    country_name <- country_code
  }
  
  # Add Country column if it doesn't exist
  if (!"Country" %in% names(df)) {
    df[, Country := country_name]
  }
  
  # Add Country_code column if it doesn't exist
  if (!"Country_code" %in% names(df)) {
    df[, Country_code := country_code]
  }
  
  # Process tender_cpv to get product market
  if ("tender_cpv" %in% names(df)) {
    # Extract first 2 characters
    df[, cpv_first2 := substr(tender_cpv, 1, 2)]
    
    # Convert to numeric (handles "03" -> 3, "09" -> 9, etc.)
    df[, cpv_code_numeric := as.numeric(cpv_first2)]
    
    # Merge with correspondence table
    df <- merge(df, 
                cpv_labels[, .(cpv_code, product_market_short_name)],
                by.x = "cpv_code_numeric",
                by.y = "cpv_code",
                all.x = TRUE,
                sort = FALSE)
    
    # Clean up temporary columns
    df[, c("cpv_first2", "cpv_code_numeric") := NULL]
    
  } else {
    warning("Column 'tender_cpv' not found. Creating NA product_market_short_name.")
    df[, product_market_short_name := NA_character_]
  }
  
  # Ensure bid_price exists (renamed from Dashboard_price_var if needed)
  if (!"bid_price" %in% names(df) && "Dashboard_price_var" %in% names(df)) {
    df[, bid_price := Dashboard_price_var]
  }
  
  # Ensure tender_year exists (renamed from Year if needed)
  if (!"tender_year" %in% names(df) && "Year" %in% names(df)) {
    df[, tender_year := Year]
  }
  
  # Generate mock indicator values
  n_rows <- nrow(df)
  
  for (indicator in list_of_indicators) {
    # Generate random values: 
    # - 70% chance of 0 or 1 (35% each)
    # - 30% chance of NA
    random_probs <- runif(n_rows)
    
    df[, (indicator) := fcase(
      random_probs < 0.30, NA_real_,      # 30% NA
      random_probs < 0.65, 0,             # 35% zero
      default = 1                          # 35% one
    )]
  }
  
  cat(sprintf("  Added %d indicator columns\n", length(list_of_indicators)))
  cat(sprintf("  Total rows: %d\n", n_rows))
  
  # Save with new filename
  output_filename <- paste0(country_code, "_mockind_export.csv")
  output_path <- file.path(output_folder, output_filename)
  
  fwrite(df, output_path)
  cat(sprintf("  Saved: %s\n", output_filename))
}

cat("\n=== Mock data generation complete! ===\n")