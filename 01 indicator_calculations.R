## ============================================================== ##
##         ProACT Dashboard Pipeline — Script 01: Indicators       ##
## ============================================================== ##

## PURPOSE:
##   This script calculates indicators at the contract level and exports one file per country. The exported
##   files are then used as input for the aggregation pipeline (Script 02).
##
## PIPELINE OVERVIEW:
##   Script 01 (this file) → Script 02 (aggregation)
##   Input:  <country>_export.csv  (one file per country, raw data)
##   Output: <country>_export.csv  (one file per country, with indicators)

# -------------------------------------------------- 
# Configuration
# -------------------------------------------------- 
data_dir <- Sys.getenv("PROACT_DATA_DIR", "/var/tmp/ivana") #location of input data 
export_dir <- Sys.getenv("PROACT_EXPORT_DIR", "/var/tmp/ivana/Export 6") #location to save export with calculated indicators (to be used as input folder for the pipeline 02)

# Risk scores for already calculated risks
RISK_NONE <- 100L
RISK_MEDIUM <- 50L
RISK_HIGH <- 0L

# -------------------------------------------------- 
# Packages
# -------------------------------------------------- 

# Install any missing packages with:
#   install.packages(c("data.table","dplyr","stringr","purrr",
#                      "tidyr","readr","janitor"))

library(data.table)
library(dplyr)
library(stringr)
library(purrr)
library(tidyr)
library(readr)
library(janitor)

# -------------------------------------------------- 
# Indicators
# -------------------------------------------------- 

#Transparency
TR_INDICATOR_VARS <- c(
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
  "ind_tr_contract_value"
)

IND_TR_VARS <- c(TR_INDICATOR_VARS, "ind_tr_index", "ind_tr_benford")

#Openness
IND_OP_VARS <- c(
  "ind_op_adv_period",
  "ind_op_short_adv_flag",
  "ind_op_lots_usage"
)

#Administrative efficiency
IND_ADM_VARS <- c(
  "ind_adm_dec_period",
  "ind_adm_long_dec_flag"
)

#Competition
IND_COMP_VARS <- c(
  "ind_comp_avg_bids",
  "ind_comp_single_bid",
  "ind_comp_good_comp",
  "ind_comp_bidder_non_l",
  "ind_comp_foreign_suppliers",
  "ind_comp_tax_haven_suppliers"
)

# -------------------------------------------------- 
# Helper Functions
# -------------------------------------------------- 

# Checking if value is present (not NA and not empty string)
present01 <- function(x) {
  as.integer(!is.na(x) & trimws(as.character(x)) != "")
}

# Checking if data.table has all specified columns
has_cols <- function(dt, cols) {
  all(cols %in% names(dt))
}

# Tabulating variables for diagnostic output
tabulate_vars <- function(vars, dt) {
  vars_ok <- intersect(vars, names(dt))
  setNames(
    lapply(vars_ok, function(v) table(dt[[v]], useNA = "ifany")),
    vars_ok
  )
}

# -------------------------------------------------- 
# Data Loading
# -------------------------------------------------- 

# Loading country data
# each dataset is named starting with two-letter country code (e.g., "AM", "US")
# this loads all country export files from data_dir folder

country_data <- function(country_code, data_dir) {
  file_path <- file.path(data_dir, paste0(country_code, "_export.csv"))
  
  if (!file.exists(file_path)) {
    warning("File not found for country: ", country_code, " at ", file_path)
    return(NULL)
  }
  
  dt <- tryCatch({
    fread(
      file_path,
      keepLeadingZeros = TRUE,
      encoding = "UTF-8",
      na.strings = c("", "-", "NA")
    )
  }, error = function(e) {
    warning("Error reading file for ", country_code, ": ", e$message)
    return(NULL)
  })
  
  return(dt)
}

# --------------------------------------------------
# Procedure Type Filter
# --------------------------------------------------

# Create proc_type_filter from ind_corr_nonopen_proc_method. This variable is used for sample restrictions.
# Maps ind_corr_nonopen_proc_method risk scores to procedure type labels:
#   100 -> "OPEN"
#   50  -> "RESTRICTED"
#   0   -> "DIRECT"
#   NA / other -> NA_character_

create_proc_type_filter <- function(dt) {
  setDT(dt)
  
  dt[, proc_type_filter := NA_character_]
  
  if ("ind_corr_nonopen_proc_method" %in% names(dt)) {
    dt[, proc_type_filter := fcase(
      as.integer(ind_corr_nonopen_proc_method) == RISK_NONE,   "OPEN",
      as.integer(ind_corr_nonopen_proc_method) == RISK_MEDIUM, "RESTRICTED",
      as.integer(ind_corr_nonopen_proc_method) == RISK_HIGH,   "DIRECT",
      default = NA_character_
    )]
  }
  
  return(dt)
}

# -------------------------------------------------- 
# Sample Restriction 
# -------------------------------------------------- 

# Create sample restriction filter variables
# Creates binary (0/1) filter variables to track sample restrictions:
# - filter_open: 1 if procedure is OPEN, 0 otherwise
# - filter_competitive: 1 if procedure is OPEN or RESTRICTED, 0 otherwise

create_sample_filters <- function(dt) {
  setDT(dt)
  
  dt[, `:=`(
    filter_open = 0L,
    filter_competitive = 0L
  )]
  
  if ("proc_type_filter" %in% names(dt)) {
    dt[proc_type_filter == "OPEN",                        filter_open        := 1L]
    dt[proc_type_filter %in% c("OPEN", "RESTRICTED"),     filter_competitive := 1L]
  }
  
  return(dt)
}

# -------------------------------------------------- 
# 1. Transparency Indicators
# -------------------------------------------------- 

# Calculating transparency indicators
# - Most indicators apply to ALL rows (all awarded contracts)
# - Competitive-only indicators: bidder_id, bidder_loc, call_pub, bid_deadline, bid_opening
# - Supplier_id is ALL rows (based on bidder_id)
# - Supplier_loc is ALL rows (based on bidder_city OR bidder_nuts)
# - Bidder_loc is COMPETITIVE ONLY (based on bidder_city OR bidder_nuts)
# - Transparency index includes ALL indicators except Benford
# - Index is row-mean over available indicators (skip NAs)

calc_transparency_indicators <- function(dt) {
  setDT(dt)
  
  # ------------------------- 
  # 0) Initialize indicators
  # ------------------------- 
  dt[, `:=`(
    ind_tr_proc_type = 0L,
    ind_tr_buyer_id = 0L,
    ind_tr_supplier_id = 0L,
    ind_tr_bidder_id = NA_integer_,
    ind_tr_call_pub = NA_integer_,
    ind_tr_bid_deadline = NA_integer_,
    ind_tr_bid_opening = NA_integer_,
    ind_tr_award_pub = 0L,
    ind_tr_prod_code = 0L,
    ind_tr_buyer_loc = 0L,
    ind_tr_supplier_loc = 0L,
    ind_tr_impl_loc = 0L,
    ind_tr_bidder_loc = NA_integer_,
    ind_tr_contract_value = 0L,
    ind_tr_benford = NA_integer_
  )]
  
  # ------------------------- 
  # 1) ALL rows indicators
  # ------------------------- 
  if ("tender_proceduretype" %in% names(dt)) {
    dt[, ind_tr_proc_type := present01(tender_proceduretype)]
  }
  
  if ("buyer_id" %in% names(dt)) {
    dt[, ind_tr_buyer_id := present01(buyer_id)]
  }
  
  # Supplier ID (ALL rows): from bidder_id
  if ("bidder_id" %in% names(dt)) {
    dt[, ind_tr_supplier_id := present01(bidder_id)]
  }
  
  if ("tender_publications_firstdcontractawarddate" %in% names(dt)) {
    dt[, ind_tr_award_pub := present01(tender_publications_firstdcontractawarddate)]
  }
  
  # Buyer location (ALL rows): any of buyer_nuts/buyer_city/buyer_postcode
  buyer_loc_cols <- intersect(c("buyer_nuts", "buyer_city", "buyer_postcode"), names(dt))
  if (length(buyer_loc_cols) > 0) {
    dt[, ind_tr_buyer_loc := as.integer(
      Reduce(`|`, lapply(.SD, function(col) present01(col) == 1L))
    ), .SDcols = buyer_loc_cols]
  }
  
  # Supplier location (ALL rows): bidder_city OR bidder_nuts
  supplier_loc_cols <- intersect(c("bidder_city", "bidder_nuts"), names(dt))
  if (length(supplier_loc_cols) > 0) {
    dt[, ind_tr_supplier_loc := as.integer(
      Reduce(`|`, lapply(.SD, function(col) present01(col) == 1L))
    ), .SDcols = supplier_loc_cols]
  }
  
  if ("tender_addressofimplementation_nuts" %in% names(dt)) {
    dt[, ind_tr_impl_loc := present01(tender_addressofimplementation_nuts)]
  }
  
  if ("bid_priceusd" %in% names(dt)) {
    dt[, ind_tr_contract_value := present01(bid_priceusd)]
  }
  
  # Product code (ALL rows) - exclude generic codes
  if ("lot_productcode" %in% names(dt)) {
    dt[, ind_tr_prod_code := as.integer({
      pc <- trimws(as.character(lot_productcode))
      !is.na(pc) & pc != "" & !(pc %in% c("99000000", "99100000", "99200000", "99300000"))
    })]
  }
  
  # ------------------------- 
  # 2) COMPETITIVE ONLY (OPEN/RESTRICTED)
  # ------------------------- 
  if ("proc_type_filter" %in% names(dt)) {
    dt[, competitive := !is.na(proc_type_filter) & proc_type_filter %in% c("OPEN", "RESTRICTED")]
    
    # Bidder ID (competitive only)
    if ("bidder_id" %in% names(dt)) {
      dt[competitive == TRUE, ind_tr_bidder_id := present01(bidder_id)]
    } else {
      dt[competitive == TRUE, ind_tr_bidder_id := 0L]
    }
    
    # Bidder location (competitive only): bidder_city OR bidder_nuts
    bidder_loc_cols <- intersect(c("bidder_city", "bidder_nuts"), names(dt))
    if (length(bidder_loc_cols) > 0) {
      dt[competitive == TRUE, ind_tr_bidder_loc := as.integer(
        Reduce(`|`, lapply(.SD, function(col) present01(col) == 1L))
      ), .SDcols = bidder_loc_cols]
    } else {
      dt[competitive == TRUE, ind_tr_bidder_loc := 0L]
    }
    
    if ("tender_publications_firstcallfortenderdate" %in% names(dt)) {
      dt[competitive == TRUE, ind_tr_call_pub := present01(tender_publications_firstcallfortenderdate)]
    } else {
      dt[competitive == TRUE, ind_tr_call_pub := 0L]
    }
    
    if ("tender_biddeadline" %in% names(dt)) {
      dt[competitive == TRUE, ind_tr_bid_deadline := present01(tender_biddeadline)]
    } else {
      dt[competitive == TRUE, ind_tr_bid_deadline := 0L]
    }
    
    # Bid opening proxy (competitive only)
    if ("notice_url" %in% names(dt)) {
      dt[competitive == TRUE, ind_tr_bid_opening := present01(notice_url)]
    } else {
      dt[competitive == TRUE, ind_tr_bid_opening := 0L]
    }
    
    dt[, competitive := NULL]
  }
  
  # ------------------------- 
  # 3) Benford anomaly (NOT in index)
  # Risk scores: 0/50 = normal (1), 100 = anomaly (0)
  # ------------------------- 
  if ("ind_corr_benfords" %in% names(dt)) {
    dt[, ind_tr_benford := fifelse(
      is.na(ind_corr_benfords),
      NA_integer_,
      fifelse(as.integer(ind_corr_benfords) %in% c(RISK_HIGH, RISK_MEDIUM), 1L,
              fifelse(as.integer(ind_corr_benfords) == RISK_NONE, 0L, NA_integer_))
    )]
  }
  
  # ------------------------- 
  # 4) Transparency index (row mean over available indicators, skip NAs)
  # ------------------------- 
  dt[, ind_tr_index := {
    m <- rowMeans(.SD, na.rm = TRUE)
    fifelse(is.nan(m), NA_real_, m)
  }, .SDcols = TR_INDICATOR_VARS]
  
  return(dt)
}

# -------------------------------------------------- 
# 2. Openness Indicators
# -------------------------------------------------- 

# Calculate openness indicators
# 1) ind_op_adv_period (days) - advertisement period (competitive only)
# 2) ind_op_short_adv_flag (0/1) - short advertisement period flag (competitive only)
# 3) ind_op_lots_usage (0/1) - tender has multiple lots (competitive only)


calc_openness_indicators <- function(dt) {
  setDT(dt)
  
  # ------------------------- 
  # 0) Initialize outputs
  # ------------------------- 
  dt[, `:=`(
    ind_op_adv_period = NA_integer_,
    ind_op_short_adv_flag = NA_integer_,
    ind_op_lots_usage = NA_integer_
  )]
  
  # ------------------------- 
  # 1) Competitive mask (OPEN or RESTRICTED procedures)
  # ------------------------- 
  dt[, competitive := FALSE]
  if ("proc_type_filter" %in% names(dt)) {
    dt[, competitive := !is.na(proc_type_filter) & proc_type_filter %in% c("OPEN", "RESTRICTED")]
  }
  
  # ------------------------- 
  # 2) Lots usage: tender-level indicator (competitive only)
  # Share of tenders divided into multiple lots (>1)
  # ------------------------- 
  if (has_cols(dt, c("tender_id", "lot_number")) && any(dt$competitive, na.rm = TRUE)) {
    tmp <- dt[competitive == TRUE, .(
      n_lots = data.table::uniqueN({
        x <- trimws(as.character(lot_number))
        x[x == ""] <- NA_character_
        x
      }, na.rm = TRUE)
    ), by = tender_id]
    
    tmp[, ind_op_lots_usage := fifelse(
      n_lots == 0,
      NA_integer_,
      as.integer(n_lots > 1)
    )]
    
    dt[competitive == TRUE, ind_op_lots_usage := tmp[.SD, on = .(tender_id), x.ind_op_lots_usage]]
  }
  
  # ------------------------- 
  # 3) Advertisement period (continuous days, competitive only)
  # ------------------------- 
  if ("submp" %in% names(dt)) {
    dt[competitive == TRUE, ind_op_adv_period := as.integer(submp)]
  }
  
  # ------------------------- 
  # 4) Short advertisement period flag (competitive only)
  # Source: ind_corr_subm_period 
  # ------------------------- 
  if ("ind_corr_subm_period" %in% names(dt)) {
    dt[competitive == TRUE, ind_op_short_adv_flag := fifelse(
      is.na(ind_corr_subm_period),
      NA_integer_,
      as.integer(as.integer(ind_corr_subm_period) %in% c(RISK_MEDIUM, RISK_HIGH))
    )]
  }
  
  dt[, competitive := NULL]
  
  return(dt)
}

# -------------------------------------------------- 
# 3. Administrative Efficiency Indicators
# -------------------------------------------------- 

# Calculate administrative efficiency indicators
# 1) ind_adm_dec_period (days) - decision period (competitive only)
# 2) ind_adm_long_dec_flag (0/1) - long decision period flag (competitive only)


calc_admin_efficiency_indicators <- function(dt) {
  setDT(dt)
  
  # ------------------------- 
  # 0) Initialize outputs
  # ------------------------- 
  dt[, `:=`(
    ind_adm_dec_period = NA_integer_,
    ind_adm_long_dec_flag = NA_integer_
  )]
  
  # ------------------------- 
  # 1) Competitive mask (OPEN or RESTRICTED)
  # ------------------------- 
  dt[, competitive := FALSE]
  if ("proc_type_filter" %in% names(dt)) {
    dt[, competitive := !is.na(proc_type_filter) & proc_type_filter %in% c("OPEN", "RESTRICTED")]
  }
  
  # ------------------------- 
  # 2) Decision period (continuous, competitive only)
  # ------------------------- 
  if ("decp" %in% names(dt)) {
    dt[competitive == TRUE, ind_adm_dec_period := as.integer(decp)]
  }
  
  # ------------------------- 
  # 3) Long decision period flag (competitive only)
  # Source: ind_corr_dec_period
  # ------------------------- 
  if ("ind_corr_dec_period" %in% names(dt)) {
    dt[competitive == TRUE, ind_adm_long_dec_flag := fifelse(
      is.na(ind_corr_dec_period),
      NA_integer_,
      as.integer(as.integer(ind_corr_dec_period) %in% c(RISK_MEDIUM, RISK_HIGH))
    )]
  }
  
  dt[, competitive := NULL]
  
  return(dt)
}

# -------------------------------------------------- 
# 4. Competition Indicators
# -------------------------------------------------- 

# Calculate competition indicators
# 1) ind_comp_avg_bids - average number of bids (OPEN only)
# 2) ind_comp_good_comp - good competition level: 6+ bids (OPEN only)
# 3) ind_comp_single_bid - single bidding flag (OPEN only)
# 4) ind_comp_bidder_non_l - non-local supplier (ALL rows)
# 5) ind_comp_foreign_suppliers - foreign supplier (ALL rows)
# 6) ind_comp_tax_haven_suppliers - tax haven supplier (ALL rows)


calc_competition_indicators <- function(dt) {
  setDT(dt)
  
  # ------------------------- 
  # 0) Initialize outputs (only if missing)
  # ------------------------- 
  init_if_missing <- function(name, value) {
    if (!(name %in% names(dt))) dt[, (name) := value]
  }
  
  init_if_missing("ind_comp_avg_bids", NA_real_)
  init_if_missing("ind_comp_single_bid", NA_integer_)
  init_if_missing("ind_comp_good_comp", NA_integer_)
  init_if_missing("ind_comp_bidder_non_l", NA_integer_)
  init_if_missing("ind_comp_foreign_suppliers", NA_integer_)
  init_if_missing("ind_comp_tax_haven_suppliers", NA_integer_)
  
  # ------------------------- 
  # 1) Eligibility mask: OPEN only (for bid-related indicators)
  # ------------------------- 
  dt[, eligible_open := FALSE]
  if ("proc_type_filter" %in% names(dt)) {
    dt[, eligible_open := !is.na(proc_type_filter) & proc_type_filter == "OPEN"]
  }
  
  # ------------------------- 
  # 2) bid count column
  # ------------------------- 
  bids_col <- NA_character_
  if ("ind_comp_bids_count" %in% names(dt)) {
    bids_col <- "ind_comp_bids_count"
  } else if ("bid_number" %in% names(dt)) {
    bids_col <- "bid_number"
  }
  
  # ------------------------- 
  # 3) Average number of bids (OPEN only)
  # ------------------------- 
  if (!is.na(bids_col)) {
    dt[eligible_open == TRUE, ind_comp_avg_bids := fifelse(
      is.na(get(bids_col)),
      NA_real_,
      as.numeric(get(bids_col))
    )]
  }
  
  # ------------------------- 
  # 4) Good competition level: 6+ bids (OPEN only)
  # ------------------------- 
  if (!is.na(bids_col)) {
    dt[eligible_open == TRUE, ind_comp_good_comp := {
      x <- suppressWarnings(as.integer(get(bids_col)))
      reliable <- !is.na(x) & x > 0L
      x_cap <- pmin(x, 50L)  # Simple winsorisation
      fifelse(!reliable, NA_integer_, as.integer(x_cap >= 6L))
    }]
  }
  
  # ------------------------- 
  # 5) Single bidding (OPEN only)
  # Source: ind_corr_singleb 
  # ------------------------- 
  if ("ind_corr_singleb" %in% names(dt)) {
    dt[eligible_open == TRUE, ind_comp_single_bid := fifelse(
      is.na(ind_corr_singleb),
      NA_integer_,
      fifelse(ind_corr_singleb == RISK_HIGH, 1L,
              fifelse(ind_corr_singleb == RISK_NONE, 0L, NA_integer_))
    )]
  }
  
  # ------------------------- 
  # 6) Foreign suppliers (ALL rows)
  # ------------------------- 
  if (has_cols(dt, c("bidder_country", "tender_country"))) {
    dt[, bidder_country_clean := toupper(trimws(as.character(bidder_country)))]
    dt[, tender_country_clean := toupper(trimws(as.character(tender_country)))]
    
    dt[, ind_comp_foreign_suppliers := fifelse(
      bidder_country_clean == "" | tender_country_clean == "",
      NA_integer_,
      fifelse(
        is.na(bidder_country_clean) | is.na(tender_country_clean),
        NA_integer_,
        as.integer(bidder_country_clean != tender_country_clean)
      )
    )]
    
    dt[, c("bidder_country_clean", "tender_country_clean") := NULL]
  }
  
  # ------------------------- 
  # 7) Non-local supplier (ALL rows)
  # ------------------------- 
  if ("ind_comp_bidder_non_local" %in% names(dt)) {
    dt[, ind_comp_bidder_non_l := fifelse(
      is.na(ind_comp_bidder_non_local),
      NA_integer_,
      fifelse(ind_comp_bidder_non_local == RISK_NONE, 1L,
              fifelse(ind_comp_bidder_non_local == RISK_HIGH, 0L, NA_integer_))
    )]
  }
  
  # ------------------------- 
  # 8) Tax haven supplier (ALL rows)
  # Source: ind_corr_taxhaven 
  # ------------------------- 
  if ("ind_corr_taxhaven" %in% names(dt)) {
    dt[, ind_comp_tax_haven_suppliers := fifelse(
      is.na(ind_corr_taxhaven),
      NA_integer_,
      fifelse(ind_corr_taxhaven == RISK_HIGH, 1L,
              fifelse(ind_corr_taxhaven == RISK_NONE, 0L, NA_integer_))
    )]
  }
  
  dt[, eligible_open := NULL]
  
  return(dt)
}

# -------------------------------------------------- 
# Main Processing Function
# -------------------------------------------------- 

# Process a single country: load data and calculate all indicators

run_country <- function(cc, data_dir) {
  message("Processing country: ", cc)
  
  # Load data
  dt <- country_data(country_code = cc, data_dir = data_dir)
  
  if (is.null(dt)) {
    warning("Skipping country ", cc, " due to data loading error")
    return(NULL)
  }
  
  setDT(dt)
  
  # ------------------------- 
  # Country-specific filters
  # ------------------------- 
  # Indonesia: keep only winning bids (awarded contracts)
  if (cc == "ID" && "bid_iswinning" %in% names(dt)) {
    dt <- dt[tolower(trimws(as.character(bid_iswinning))) %in% c("true", "t", "1", "yes", "y")]
  }
  
  # -------------------------
  # CREATE PROC TYPE FILTER FIRST
  # -------------------------
  dt <- create_proc_type_filter(dt)
  
  # ------------------------- 
  # CREATE SAMPLE FILTERS
  # (before computing indicators, so they're available for diagnostics)
  # ------------------------- 
  dt <- create_sample_filters(dt)
  
  # ------------------------- 
  # Compute all indicator groups
  # ------------------------- 
  dt <- calc_transparency_indicators(dt)
  dt <- calc_openness_indicators(dt)
  dt <- calc_admin_efficiency_indicators(dt)
  dt <- calc_competition_indicators(dt)
  
  # ------------------------- 
  # Generate diagnostic tables
  # ------------------------- 
  list(
    data = dt,
    tables_tr = tabulate_vars(IND_TR_VARS, dt),
    tables_op = tabulate_vars(IND_OP_VARS, dt),
    tables_adm = tabulate_vars(IND_ADM_VARS, dt),
    tables_comp = tabulate_vars(IND_COMP_VARS, dt)
  )
}

# -------------------------------------------------- 
# Run pipeline for all countries
# -------------------------------------------------- 

# Country list
COUNTRY_CODES <- c(
  "AM", "AT", "BE", "BG", "CH", "CL", "CO", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR",
  "GE", "GR", "HR", "HU", "ID", "IE", "IN", "IS", "IT", "JM", "KE", "LT", "LU", "LV", "MD",
  "MK", "MT", "MX", "NL", "NO", "PL", "PT", "PY", "RO", "SE", "SI", "SK", "UG", "UK", "US", "UY"
)

# Process all countries
message("=== Starting ProACT Indicator Pipeline ===")
message("Processing ", length(COUNTRY_CODES), " countries...")

results <- setNames(
  lapply(COUNTRY_CODES, function(cc) {
    tryCatch(
      run_country(cc, data_dir),
      error = function(e) {
        warning("Failed to process country ", cc, ": ", e$message)
        return(NULL)
      }
    )
  }),
  COUNTRY_CODES
)

# Count successes
successful <- sum(sapply(results, function(x) !is.null(x)))
message("Successfully processed ", successful, " out of ", length(COUNTRY_CODES), " countries")

# -------------------------------------------------- 
# Export Results
# -------------------------------------------------- 

message("=== Exporting Results ===")
dir.create(export_dir, recursive = TRUE, showWarnings = FALSE)

export_count <- 0
for (cc in COUNTRY_CODES) {
  if (is.null(results[[cc]])) {
    message("Skipping export for ", cc, " (no data)")
    next
  }
  
  tryCatch({
    out_file <- file.path(export_dir, paste0(cc, "_export.csv"))
    fwrite(results[[cc]]$data, out_file)
    export_count <- export_count + 1
    message("Exported: ", cc)
  }, error = function(e) {
    warning("Failed to export ", cc, ": ", e$message)
  })
}

message("=== Pipeline Complete ===")
message("Exported ", export_count, " country files to: ", export_dir)

# -------------------------------------------------- 
# Country level results/diagnostics
# -------------------------------------------------- 
# Example:
#dt_am <- results$AM$data #data frame for Armenia
#tables_tr_am <- results$AM$tables_tr #overview of transparency indicators for armenia

# Using the sample restriction filters:
#table(dt_am$filter_open)           # Count of observations in OPEN procedures
#table(dt_am$filter_competitive)    # Count of observations in COMPETITIVE (OPEN + RESTRICTED) procedures

# Analyze indicators within specific samples:
#mean(dt_am[filter_open == 1, ind_comp_avg_bids], na.rm = TRUE) #mean value of indicator within open procedures filter


