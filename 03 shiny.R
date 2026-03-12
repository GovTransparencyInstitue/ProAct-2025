library(shiny)

# ----------------------------
# Helpers
# ----------------------------
to_num <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x[x %in% c("", "NA", "N/A", "na", "n/a", "-", "NULL", "null")] <- NA
  x <- gsub("[^0-9.-]", "", x)
  x[x == ""] <- NA
  suppressWarnings(as.numeric(x))
}

info_label <- function(label, text) {
  tags$div(
    style = "display: flex; align-items: center; justify-content: space-between;",
    tags$span(label),
    tags$span(
      icon("info-circle"),
      class = "info-tooltip",
      style = "margin-left: 10px;",
      `data-toggle` = "tooltip",
      `data-placement` = "right",
      title = text
    )
  )
}

normalize_proc_type <- function(x) {
  x <- trimws(toupper(as.character(x)))
  ifelse(
    x %in% c("OPEN", "RESTRICTED", "DIRECT"),
    paste0(toupper(substr(x, 1, 1)), tolower(substr(x, 2, nchar(x)))),
    NA_character_
  )
}

normalize_proc_scope <- function(x) {
  x <- trimws(toupper(as.character(x)))
  ifelse(
    x %in% c("OPEN", "COMPETITIVE", "ALL"),
    x,
    NA_character_
  )
}

proc_choices_from_scope <- function(scope) {
  if (is.null(scope) || length(scope) == 0 || is.na(scope) || scope == "") {
    return(c("Open", "Restricted", "Direct"))
  }
  
  switch(
    scope,
    "OPEN"        = c("Open"),
    "COMPETITIVE" = c("Open", "Restricted"),
    "ALL"         = c("Open", "Restricted", "Direct"),
    c("Open", "Restricted", "Direct")
  )
}

# ----------------------------
# 1) Read data
# ----------------------------
raw_data <- read.csv("ProACT_dashboard_export.csv", stringsAsFactors = FALSE)

# ----------------------------
# 2) Map/rename columns
# ----------------------------
data <- within(raw_data, {
  country              <- Country
  indicator            <- Indicator
  year                 <- as.integer(tender_year)
  sector               <- product_market_short_name
  supplytype           <- tender_supplytype
  contract_size        <- Contract_value
  proc_type            <- normalize_proc_type(proc_type_filter)
  indicator_proc_scope <- normalize_proc_scope(indicator_proc_scope)
  
  # Measures
  indicator_value           <- to_num(Indicator_value)
  share_number_of_contracts <- to_num(share_number_of_contracts)
  share_contract_value      <- to_num(share_contract_value)
})

# Keep only the columns the app uses
data <- data[, c(
  "country", "indicator", "year", "sector", "supplytype", "contract_size",
  "proc_type", "indicator_proc_scope",
  "indicator_value", "share_number_of_contracts", "share_contract_value"
)]

# Drop rows with missing filter essentials (NOT dropping measure columns)
data <- data[complete.cases(data[, c(
  "country", "indicator", "year", "sector", "supplytype", "contract_size"
)]), ]

# ----------------------------
# 3) UI choices
# ----------------------------
all_countries      <- sort(unique(data$country))
all_indicators     <- sort(unique(data$indicator))
all_sectors        <- sort(unique(data$sector))
all_supplytypes    <- sort(unique(data$supplytype))
all_contract_size  <- sort(unique(data$contract_size))
all_proc_types     <- c("Open", "Restricted", "Direct")
all_years          <- sort(unique(data$year))

all_contract_size  <- sort(unique(data$contract_size))

contract_size_choices <- c(
  "All" = "All",
  "HIGH: \u2265 400,000 USD" = "HIGH",
  "MEDIUM: 50,000\u2013399,999 USD" = "MED",
  "LOW: < 50,000 USD" = "LOW"
)

# only fall back if the expected coded values are not present at all
if (!all(c("HIGH", "MED", "LOW") %in% all_contract_size)) {
  contract_size_choices <- c("All" = "All", stats::setNames(all_contract_size, all_contract_size))
}

# ----------------------------
# 4) Load indicator descriptions
# ----------------------------
indicator_meta <- read.csv("indicator_descriptions.csv", stringsAsFactors = FALSE)
indicator_desc <- setNames(indicator_meta$description, indicator_meta$indicator_name)

# ----------------------------
# UI
# ----------------------------
ui <- fluidPage(
  titlePanel("WB Proact dashboard"),
  div(style = "margin-bottom: 40px"),
  
  tags$head(
    tags$style(HTML("
      .info-tooltip { cursor: pointer; color: #adadad; }
      .info-tooltip i { pointer-events: none; }
      body, label, input, button, select, textarea { font-family: Arial, Helvetica, sans-serif; }
      h1, h2, h3, h4, h5, h6 { font-family: Arial, Helvetica, sans-serif; }
      .fa-info-circle { margin-left: 6px; }
      .info-tooltip { cursor: help; color: #adadad; }
      .tooltip-inner { text-align: left; max-width: 260px; }
    ")),
    tags$script(HTML("
      $(function () {
        $('[data-toggle=\"tooltip\"]').tooltip({ container: 'body', delay: { show: 0, hide: 0 } });
      });
    "))
  ),
  
  sidebarLayout(
    sidebarPanel(
      width = 3,
      
      selectInput(
        "indicator", "Indicator",
        choices = all_indicators,
        selected = all_indicators[1]
      ),
      
      selectInput(
        "measure",
        info_label(
          "Measure",
          "Share with respect to number of contracts and Share with respect to total contract value are active only for binary indicators."
        ),
        choices = c(
          "Indicator value" = "indicator_value",
          "Share with respect to number of contracts" = "share_number_of_contracts",
          "Share with respect to total contract value" = "share_contract_value"
        ),
        selected = "indicator_value"
      ),
      
      selectInput(
        inputId = "base_country",
        label   = info_label(
          "Base country",
          "The base country is the primary country of interest. Its bar is highlighted in green in the chart and serves as the reference point for comparison against the selected comparator countries."
        ),
        choices = all_countries,
        selected = all_countries[1]
      ),
      
      selectizeInput(
        inputId = "comparator_countries",
        label   = info_label(
          "Comparator countries",
          "Selected countries that are shown alongside the base country in the chart. Their bars are displayed in grey."
        ),
        choices  = setdiff(all_countries, all_countries[1]),
        selected = setdiff(all_countries[min(2, length(all_countries))], all_countries[1]),
        multiple = TRUE,
        options  = list(
          placeholder = "Select one or more countries"
        )
      ),
      
      selectInput(
        inputId = "year",
        label   = info_label(
          "Year",
          "The tender year is assigned based on the first available valid date, using a fallback order of the tender’s award date, contract signature date, award decision date, bid deadline, and finally the call for tender publication date"
        ),
        choices = c("All", all_years),
        selected = "All"
      ),
      
      selectInput(
        inputId = "sector",
        label   = info_label(
          "Sector",
          "Sectors are assigned using the first two digits of the CPV code, which are mapped to standardized, shortened sector names; for countries using non-CPV product codes, CPV codes are first assigned before matching tenders to sector labels."
        ),
        choices = c("All", all_sectors),
        selected = "All"
      ),
      
      selectInput(
        inputId = "supplytype",
        label   = info_label(
          "Supply type",
          "Supply type classifies each tender by the nature of what is procured—Services, Supplies, or Works—indicating whether the contract concerns service provision, the purchase of goods, or the execution of construction or infrastructure works."
        ),
        choices = c("All", all_supplytypes),
        selected = "All"
      ),
      
      selectInput(
        inputId = "proc_type",
        label   = info_label(
          "Procedure Type",
          "Procedure type is restricted based on the selected indicator scope: OPEN allows only Open; COMPETITIVE allows Open and Restricted; ALL allows Open, Restricted, and Direct."
        ),
        choices = c("All", all_proc_types),
        selected = "All"
      ),
      
      selectInput(
        inputId = "contract_size",
        label   = info_label(
          "Contract size",
          "Contract size categories are defined as HIGH: \u2265 400,000 USD; MEDIUM: 50,000\u2013399,999 USD; LOW: < 50,000 USD."
        ),
        choices = contract_size_choices,
        selected = "All"
      )
    ),
    
    mainPanel(
      width = 9,
      div(
        class = "main-panel",
        div(
          style = "width: 100%; padding-right: 40px;",
          uiOutput("bar_chart_ui")
        )
      )
    )
  )
)

# ----------------------------
# Server
# ----------------------------
server <- function(input, output, session) {
  
  # Keep comparator choices synced: exclude base country and remove it if selected
  observeEvent(input$base_country, {
    available_comparators <- setdiff(all_countries, input$base_country)
    selected_comparators  <- setdiff(input$comparator_countries, input$base_country)
    
    updateSelectizeInput(
      session,
      "comparator_countries",
      choices  = available_comparators,
      selected = selected_comparators,
      server   = TRUE
    )
  }, ignoreInit = TRUE)
  
  current_indicator_scope <- reactive({
    scopes <- unique(na.omit(data$indicator_proc_scope[data$indicator == input$indicator]))
    if (length(scopes) == 0) {
      return("ALL")
    }
    scopes[1]
  })
  
  # Update supply type choices based on other filters
  observeEvent(list(input$indicator, input$year, input$sector, input$contract_size, input$proc_type), {
    
    df <- data
    df <- df[df$indicator == input$indicator, ]
    
    if (!is.null(input$year) && input$year != "All") {
      df <- df[df$year == as.integer(input$year), ]
    }
    
    if (input$sector != "All") {
      df <- df[df$sector == input$sector, ]
    }
    
    if (input$contract_size != "All") {
      df <- df[df$contract_size == input$contract_size, ]
    }
    
    if (!is.null(input$proc_type) && input$proc_type != "All") {
      df <- df[df$proc_type == input$proc_type, ]
    }
    
    supply_choices <- sort(unique(df$supplytype))
    supply_choices <- supply_choices[!is.na(supply_choices) & supply_choices != ""]
    
    selected <- input$supplytype
    if (is.null(selected) || !(selected %in% supply_choices)) selected <- "All"
    
    updateSelectInput(
      session,
      "supplytype",
      choices  = c("All", supply_choices),
      selected = selected
    )
  }, ignoreInit = TRUE)
  
  # Update procedure type choices based on indicator scope + other filters
  observeEvent(list(input$indicator, input$year, input$sector, input$contract_size, input$supplytype), {
    
    allowed_proc_types <- proc_choices_from_scope(current_indicator_scope())
    
    df <- data
    df <- df[df$indicator == input$indicator, ]
    
    if (!is.null(input$year) && input$year != "All") {
      df <- df[df$year == as.integer(input$year), ]
    }
    
    if (input$sector != "All") {
      df <- df[df$sector == input$sector, ]
    }
    
    if (input$contract_size != "All") {
      df <- df[df$contract_size == input$contract_size, ]
    }
    
    if (!is.null(input$supplytype) && input$supplytype != "All") {
      df <- df[df$supplytype == input$supplytype, ]
    }
    
    proc_choices <- sort(unique(df$proc_type))
    proc_choices <- proc_choices[!is.na(proc_choices) & proc_choices != ""]
    proc_choices <- intersect(allowed_proc_types, proc_choices)
    
    # if filtered data is empty, still enforce scope-based choices
    if (length(proc_choices) == 0) {
      proc_choices <- allowed_proc_types
    }
    
    selected <- input$proc_type
    if (is.null(selected) || !(selected %in% proc_choices)) {
      selected <- if (length(proc_choices) == 1) proc_choices[1] else "All"
    }
    
    updateSelectInput(
      session,
      "proc_type",
      choices  = if (length(proc_choices) == 1) proc_choices else c("All", proc_choices),
      selected = selected
    )
  }, ignoreInit = FALSE)
  
  filtered_data <- reactive({
    df <- data
    
    df <- df[df$indicator == input$indicator, ]
    
    if (!is.null(input$year) && input$year != "All") {
      df <- df[df$year == as.integer(input$year), ]
    }
    
    if (input$sector != "All") {
      df <- df[df$sector == input$sector, ]
    }
    
    if (input$contract_size != "All") {
      df <- df[df$contract_size == input$contract_size, ]
    }
    
    if (input$supplytype != "All") {
      df <- df[df$supplytype == input$supplytype, ]
    }
    
    if (!is.null(input$proc_type) && input$proc_type != "All") {
      df <- df[df$proc_type == input$proc_type, ]
    }
    
    df
  })
  
  output$bar_chart_ui <- renderUI({
    n <- 1 + length(input$comparator_countries)
    
    px_per_bar <- 28
    extra_px <- 220
    h <- max(550, n * px_per_bar + extra_px)
    
    plotOutput("bar_chart", height = paste0(h, "px"))
  })
  
  output$bar_chart <- renderPlot({
    df <- filtered_data()
    
    selected_countries <- unique(c(input$base_country, input$comparator_countries))
    df <- df[df$country %in% selected_countries, ]
    
    if (nrow(df) == 0) {
      plot.new()
      title("No data for the selected filters")
      return()
    }
    
    measure_col <- input$measure
    
    agg_fun <- switch(
      measure_col,
      "indicator_value" = mean,
      "share_number_of_contracts" = mean,
      "share_contract_value" = mean,
      mean
    )
    
    mf <- data.frame(
      country = df$country,
      m = df[[measure_col]],
      stringsAsFactors = FALSE
    )
    mf <- mf[!is.na(mf$country) & mf$country != "", , drop = FALSE]
    
    safe_agg <- function(x) {
      if (all(is.na(x))) return(NA_real_)
      agg_fun(x, na.rm = TRUE)
    }
    
    agg <- aggregate(
      m ~ country,
      data = mf,
      FUN = safe_agg,
      na.action = na.pass
    )
    names(agg)[2] <- "value"
    
    agg <- agg[is.finite(agg$value), , drop = FALSE]
    
    if (nrow(agg) == 0) {
      plot.new()
      title("No valid data for selected filters")
      return()
    }
    
    agg <- agg[order(agg$value, decreasing = TRUE), , drop = FALSE]
    
    bar_cols  <- ifelse(agg$country == input$base_country, "#1cad82", "#e0e0e0")
    
    par(family = "Arial")
    par(mar = c(4, 12, 6, 1))
    
    indicator_parts <- strsplit(input$indicator, "-", fixed = TRUE)[[1]]
    title_expr <- if (length(indicator_parts) > 1) {
      bquote(bold(.(indicator_parts[1])) * "-" * .(paste(indicator_parts[-1], collapse = "-")))
    } else {
      bquote(bold(.(input$indicator)))
    }
    
    desc_raw <- indicator_desc[[input$indicator]]
    desc <- if (!is.null(desc_raw) && nzchar(desc_raw)) desc_raw else ""
    desc_wrapped <- paste(strwrap(desc, width = 85), collapse = "\n")
    
    max_val <- max(agg$value, na.rm = TRUE)
    if (!is.finite(max_val) || max_val == 0) max_val <- 1
    
    wrapped_names <- sapply(agg$country, function(x) {
      paste(strwrap(x, width = 18), collapse = "\n")
    })
    
    bp <- barplot(
      agg$value,
      names.arg = wrapped_names,
      horiz     = TRUE,
      las       = 1,
      col       = bar_cols,
      border    = NA,
      axes      = FALSE,
      font      = 2,
      cex.names = 1.15,
      xlim      = c(0, max_val * 1.1),
      main      = ""
    )
    
    title(
      main = title_expr,
      line = 4,
      adj  = 0,
      cex.main = 1.45,
      family = "Arial"
    )
    
    if (nzchar(desc_wrapped)) {
      mtext(
        desc_wrapped,
        side   = 3,
        line   = 1,
        adj    = 0,
        cex    = 1.05,
        family = "Arial"
      )
    }
    
    label_vals <- if (measure_col %in% c("share_number_of_contracts", "share_contract_value")) {
      paste0(format(round(agg$value, 2), nsmall = 2, big.mark = ","))
    } else {
      format(round(agg$value, 2), nsmall = 2, big.mark = ",")
    }
    
    outside_offset <- max_val * 0.01
    
    for (i in seq_len(nrow(agg))) {
      text(
        x      = agg$value[i] + outside_offset,
        y      = bp[i],
        labels = label_vals[i],
        col    = "black",
        pos    = 4,
        cex    = 1.1,
        font   = 2
      )
    }
  })
}

shinyApp(ui = ui, server = server)