
# This script provides functions that takes the anonymised PDL data 
# and produces the modelling data used for the paper

library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(fuzzyjoin)

# Tidy the report attributes data
# We will assume that belts are installed at 12:00 noon (UTC) on belt_install_date, and add a column belt_install_datetime for this
tidy_rep_attr <- function(rep_attr) {
  rep_attr %>%
    mutate(belt_install_datetime = as.POSIXct(paste0(as.character(belt_install_date), " 12:00:00"), tz = "UTC"))
}


# Tidy the utilisation data
# Reduces utilisation to a smaller, relevant subset to reduce subsequent processing time,
# and splits records across equipment to produce a table where each row is an individual piece
# of equipment involved in a movement
tidy_util <- function(util, rep_attr, tph_min = 300, tph_max = 15000, hours_min = 0) {
  
  # Remove unrealistic rows based on filters
  util_filtered <- util %>%
    mutate(start_datetime = as.POSIXct(start_datetime, tz = "UTC"),
           end_datetime = as.POSIXct(end_datetime, tz = "UTC"),
           hours = as.numeric(end_datetime - start_datetime, units = "hours")) %>%
    filter(tonnes / hours > tph_min,
           tonnes / hours < tph_max,
           hours > hours_min)
  
  # For every row in utilisation, split our eq list by commas
  util_eq <- util_filtered %>%
    pull(eq_list) %>%
    str_split(",")  
  
  # Get every unique equipment id in our report attributes
  eq_ids <- rep_attr$eq_id %>%
    str_split(",") %>%
    unlist() %>%
    unique() 
  
  # Which rows of our filtered utilisation set includes equipment that appears in our report attributes somewhere?
  indices <- map_lgl(util_eq, ~any(.x %in% eq_ids))
  util_subset <- util_filtered[indices, ] 
  
  # Split rows of utilisation by eq_list so that we can join by equipment later:
  # first remove any trailing commas from util
  util_subset$eq_list <- gsub(",$", "", util_subset$eq_list)
  
  # Split movements by equipment, this will make our movements table much longer
  util_long <- util_subset %>%
    mutate(eq_id = str_split(eq_list, ",")) %>%
    unnest(eq_id) %>%
    filter(eq_id %in% eq_ids) %>% # Try cut down this set by removing rows that don't include equipment not in our conveyor attributes
    select(-eq_list) # remove now redundant column
  util_long
}


# Tidy the thickness data
#
# removes results withink skirt_mm distance of the edges
# sets each thickness test to be a POSIXct type, assuming a time of 12:00 noon
# removes thickness tests that predate all material movements for that equipment
tidy_thickness <- function(thickness, rep_attr_tidy, util_tidy, skirt_mm = 400) {
  # Helper function to get minimum movement date in util for eq_ids 
  min_valid_test_datetime <- function(eq_id_string) {
    eq_ids <- unlist(str_split(eq_id_string, ","))
    util_tidy %>%
      filter(eq_id %in% eq_ids) %>% 
      pull(start_datetime) %>%
      min()
  }
  
  min_dtms <- purrr::map(rep_attr_tidy$eq_id, min_valid_test_datetime) %>%
    do.call(c, .)
  rep_attr_tidy$min_dtm <- min_dtms
  
  # Assume that each thickness test occurs at 12:00:00 noon on the date of testing for consistency
  thickness_tidy <- thickness %>%
    mutate(datetime = as.POSIXct(paste0(as.character(date), " 12:00:00"), tz = "UTC")) %>%
    left_join(select(rep_attr_tidy, report_id, belt_width_mm, eq_id, min_dtm), 
              by = "report_id") %>%
    filter(position > skirt_mm,
           position < belt_width_mm - skirt_mm,
           datetime >= min_dtm) %>%
    select(datetime, position, thickness_mm, report_id)
}


calculate_belt_utilisation <- function(thickness_pooled, util_tidy) {
  
  # helper function to extract utilisation for a single belt pool
  calc_util <- function(test_dtms, eq_id_string) {
    test_dtms <- sort(test_dtms)
    
    if (length(eq_id_string) > 1) {
      stop("attempt to calculate utilisation using more than one eq_id string, have you pooled 
            more than one conveyor together?")
    }
    
    eq_ids <- unlist(str_split(eq_id_string, ","))
    mvmnts <- util_tidy %>%
      filter(start_datetime >= min(test_dtms),
             start_datetime < max(test_dtms),
             eq_id %in% eq_ids) %>% 
      distinct(start_datetime, end_datetime, tonnes, product_name, .keep_all = TRUE) %>%
      arrange(start_datetime)
    
    # accumulate movments over test datetimes
    mvmnt_intervals <- purrr::map_df(test_dtms, function(test_dtm) {
      mvmnts_subset <- filter(mvmnts, start_datetime < test_dtm)
      
      lump <- mvmnts_subset %>% 
        filter(product_group == "Lump") 
      
      fines <- mvmnts_subset %>% 
        filter(product_group == "Fines")
      
      #jo-edited to include perc_fines
      data.frame(Mt_lump_cum = sum(lump$tonnes) / 1E6,
                 Mt_fines_cum = sum(fines$tonnes) / 1E6,
                 Mt_total_cum = (sum(lump$tonnes) + sum(fines$tonnes)) / 1E6,
                 n_mvmnts_cum = nrow(lump) + nrow(fines),
                 perc_fines= ifelse(is.finite(sum(fines$tonnes)/(sum(lump$tonnes) + sum(fines$tonnes))),(sum(fines$tonnes)/(sum(lump$tonnes) + sum(fines$tonnes))),0)
                )
      
    }) %>%
      mutate(datetime_ending = test_dtms,
             weeks = as.numeric(datetime_ending - min(datetime_ending), units = "weeks"))
    
    # reorder columns
    select(mvmnt_intervals, datetime_ending, weeks, Mt_fines_cum, 
           Mt_lump_cum, Mt_total_cum, n_mvmnts_cum,perc_fines)
  }
  
  belt_utilisation <- thickness_pooled %>%
    group_by(pool) %>%
    do(calc_util(unique(.$datetime),
                 unique(.$eq_id))) %>%
    ungroup()
  
  belt_utilisation
}


calculate_wear_rates_mean <- function(wear_data) {
  
  #model all data by pool and position
  wear_fits <- wear_data %>%
    group_by(pool, datetime, position) %>%
    summarise(thickness_mean = mean(thickness_mm, na.rm = TRUE),
              Mtonnes = first(Mt_total_cum),
              weeks = first(weeks)) %>%
    group_by(pool,position) %>%
    do(fit_time = lm(thickness_mean ~ weeks, data = .),
       fit_tonnes = lm(thickness_mean ~ Mtonnes, data = .))
  

  wear_rates_global <- wear_fits %>%
    gather(metric, fit, fit_time, fit_tonnes) %>%
    rowwise() %>%
    mutate(fit_rank = fit$rank) %>%
    filter(fit_rank == 2) %>% # remove models that aren't of full rank
    mutate(coef_name = names(fit$coefficients)[[2]],
           rate = -1 * fit$coefficients[[coef_name]],
           r2 = broom::glance(fit)$r.squared,
           std_err = broom::tidy(fit) %>%
             filter(term == coef_name) %>%
             pull(std.error)) %>%
    select(pool, position, metric, rate, r2, std_err) %>%
    ungroup()
  
  #Find the average of the positions for each pool
  wear_fits_mean <- wear_rates_global %>%
    group_by(pool,metric) %>%
    summarise(rate=mean(rate),r2=mean(r2),std_error=mean(std_err)) %>%
    mutate(metric = stringr::str_replace_all(metric, c("fit_time" = "mm/week",
                                                       "fit_tonnes" = "mm/MT"))) %>%
    ungroup() 

  # find the last date that readings were collected.  This will give us the percentage of fines & tonnes moved for that conveyor belt.
  lastdates_set<- wear_data %>%
    group_by(pool) %>%
    summarise(datetime=max(datetime)) 
  
  # extract the tonnages relating the most recent reading
  totalMT_set<- inner_join(wear_data[,c("datetime","pool","perc_fines","Mt_fines_cum","Mt_lump_cum")],lastdates_set,by=c("pool","datetime")) %>%
    group_by(pool) %>%
    summarise(maxdate=max(datetime),
              perc_fines=max(perc_fines),
              TotalMTFines=max(Mt_fines_cum),
              TotalMTLump=max(Mt_lump_cum))

  #merge with pool info  
  wear_rates_mean<-inner_join(wear_fits_mean,totalMT_set,"pool")
}

calculate_wear_rates_max <- function(wear_data) {
  wear_fits_global <- wear_data %>%
    group_by(pool, position) %>%
    do(fit_time = lm(thickness_mm ~ weeks, data = .),
       fit_tonnes = lm(thickness_mm ~ Mt_total_cum, data = .)) 
  
  wear_rates_global <- wear_fits_global %>%
    gather(metric, fit, fit_time, fit_tonnes) %>%
    rowwise() %>%
    mutate(fit_rank = fit$rank) %>%
    filter(fit_rank == 2) %>% # remove models that aren't of full rank
    mutate(coef_name = names(fit$coefficients)[[2]],
           rate = -1 * fit$coefficients[[coef_name]],
           r2 = broom::glance(fit)$r.squared,
           std_err = broom::tidy(fit) %>%
             filter(term == coef_name) %>%
             pull(std.error)) %>%
    select(pool, position, metric, rate, r2, std_err) %>%
    ungroup()
  
  wear_fits_max <- wear_rates_global %>%
    group_by(pool, metric) %>%
    arrange(desc(rate)) %>%
    filter(row_number() == 1) %>%
    #top_n(1, rate) %>%
    ungroup() %>%
    mutate(metric = stringr::str_replace_all(metric, c("fit_time" = "mm/week",
                                                       "fit_tonnes" = "mm/MT")))
  
  #find the last date that readings were collected
  lastdates_set<- wear_data %>%
    group_by(pool) %>%
    summarise(datetime=max(datetime)) 
  
  #extract the tonnages relating the most recent reading
  totalMT_set<- inner_join(wear_data[,c("datetime","pool","perc_fines","Mt_fines_cum","Mt_lump_cum")],lastdates_set,by=c("pool","datetime")) %>%
    group_by(pool) %>%
    summarise(maxdate=max(datetime),
              perc_fines=max(perc_fines),
              TotalMTFines=max(Mt_fines_cum),
              TotalMTLump=max(Mt_lump_cum))
  
  #merge with pool info
  wear_rates_max<-inner_join(wear_fits_max,totalMT_set,"pool")
}





