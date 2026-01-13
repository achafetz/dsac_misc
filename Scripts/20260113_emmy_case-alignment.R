# PROJECT:  dsac_misc
# PURPOSE:  align LA confirmation codes with case numbers
# AUTHOR:   A.Chafetz | CMS
# REF ID:   243b81d9 
# LICENSE:  MIT
# DATE:     2026-01-13
# UPDATED: 

# DEPENDENCIES ------------------------------------------------------------
  
  library(tidyverse)
  library(janitor)
  library(readxl)
  library(vroom)

# GLOBAL VARIABLES --------------------------------------------------------
  
  ref_id <- "243b81d9"  #a reference to be places in viz captions 
  
  #paths
  path_conf <- "Data/CBV Prod Confirmation Codes.xlsx"
  path_case <- c("Data/weekly_report_20251103-20251109.csv", "Data/weekly_report_20251201-20251207.csv")
  
  #reports of interest
  v_reports <- c("LALDHLNY559948", "LALDHLPH565139",
                 "LALDH021552671", "LALDHFLS553845",
                 "LALDHHYW547473", "LALDHKBF547846", "LALDHZSW554758", "LALDH097555040",
                 "LALDHYDZ562433", "LALDH4JW562144", 
                 "LALDHCQ2548908", "LALDHHX6558632", "LALDH5KX554397"
  )
  
# IMPORT ------------------------------------------------------------------
  
  #CBV confirmation codes from LA
  df_confirmation <- read_excel(path_conf, .name_repair = make_clean_names)
  
  #LA weekly reports
  df_cases <- vroom(path_case)
  


# MUNGE - CONFIRMATION CODES ----------------------------------------------

  #convert time to correct TZ and then convert to UTC for matching purposes
  df_confirmation <- df_confirmation %>% 
    mutate(create_dt = create_dt %>% str_extract("^.*(?=\\.)") %>% as_datetime(., tz = "America/Chicago"),
           create_dt_utc = with_tz(create_dt, tzone = "UTC")) %>% 
    select(-create_dt)

  #identify duplicate submission times (we can't accurately join these automatically)
  v_dup_times <- df_confirmation %>% 
    distinct(confirmation_code, create_dt_utc) %>%
    group_by(create_dt_utc) %>% 
    filter(n() > 1) %>% 
    ungroup() %>% 
    pull(confirmation_code)

  #remove complete dups and agg status for same time
  df_confirmation <- df_confirmation %>%
    distinct() %>% 
    group_by(confirmation_code, create_dt_utc) %>% 
    summarise(
      processing_status = paste0(processing_status, collapse = ", "), 
      .groups = "drop")
  
  #duplicate confirmations
  df_confirmation <- df_confirmation %>% 
    group_by(confirmation_code) %>% 
    mutate(n = n(),
           row = row_number()) %>% 
    ungroup() %>% 
    mutate(confirmation_code = ifelse(n == 1,
                                      str_glue("{confirmation_code}"),
                                      str_glue("{confirmation_code}_{row}"))) %>% 
    select(-n, -row)


# MUNGE - CASE CODES ------------------------------------------------------

  #convert times to date time format and note missing case numbers
  df_cases <- df_cases %>% 
    mutate(across(ends_with("_at"), \(x) ymd_hms(x)),
           case_number = ifelse(is.na(case_number), "[none]", case_number)) %>% 
    rename_with(~paste0("case_", .x),
                .cols = ends_with("_at"))

  #flag duplicate instances of the same cases
  df_cases <- df_cases %>% 
    group_by(case_number) %>% 
    mutate(n = n(),
           row = row_number()) %>% 
    ungroup() %>% 
    mutate(case_number = case_when(n == 1 ~ str_glue("{case_number}"),
                                   case_number == "[none]" ~ str_glue("[none]_{case_completed_at}"),
                                   TRUE ~ str_glue("{case_number}_{row}"))) %>% 
    select(-n, -row)
  


# MERGE DATA --------------------------------------------------------------

  #set tolerance for fuzzy time-based crossing join
  tolerance_seconds <- 5

  #complete crossing of unique times in each dataset
  df_aligned <- df_cases %>%
    crossing(df_confirmation %>% 
               group_by(create_dt_utc) %>% 
               filter(n() == 1)) 

# MUNGE - MERGED DATA -----------------------------------------------------

  #calc time difference and return only the closest result
  df_aligned <- df_aligned %>%
    mutate(time_diff_sec = difftime(create_dt_utc, case_completed_at, units = "secs") %>% as.numeric() %>% abs()) %>%
    filter(time_diff_sec <= tolerance_seconds) %>%
    group_by(case_number) %>%
    slice_min(time_diff_sec, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(case_number, case_completed_at, confirmation_code, processing_status, 
           create_dt_utc, time_diff_sec)

  #remove any duplicate codes
  df_aligned <- df_aligned %>% 
    group_by(confirmation_code) %>% 
    slice_head(n = 1) %>% 
    ungroup()

  #add back in duplicates intentionally left off from matching
  df_aligned <- df_aligned %>% 
    bind_rows(df_cases %>% 
                group_by(case_completed_at) %>% 
                filter(n() > 1) %>% 
                ungroup() %>% 
                select(case_number, case_completed_at) %>% 
                mutate(status = "not matched - submission time duplicate")) %>% 
    bind_rows(df_confirmation %>% 
                group_by(create_dt_utc) %>% 
                filter(n() > 1) %>% 
                ungroup() %>% 
                select(confirmation_code, processing_status, create_dt_utc) %>% 
                mutate(status = "not matched - submission time duplicate")) %>% 
    mutate(status = ifelse(is.na(status), "matched - fuzzy", status))
  
  #add back in unaligned data
  df_aligned <- df_aligned %>% 
    bind_rows(df_cases %>% 
                filter(!case_number %in% unique(df_aligned$case_number)) %>% 
                select(case_number, case_completed_at) %>% 
                mutate(status = "not matched")) %>% 
    bind_rows(df_confirmation %>% 
                filter(!confirmation_code %in%(df_aligned$confirmation_code)) %>% 
                select(confirmation_code, processing_status, create_dt_utc) %>% 
                mutate(status = "not matched"))
  
  #clean up
  df_aligned <- df_aligned %>%
    mutate(case_number = str_remove(case_number, ("(?<=\\]).*"))) %>% 
    separate_wider_delim(case_number, delim = "_", names = c("case_number", "case_number_instance"),  too_few = "align_start") %>% 
    separate_wider_delim(confirmation_code, delim = "_", names = c("confirmation_code", "confirmation_code_instance"),  too_few = "align_start")


  #look at just data from pilot time frame and weeks of report provided
  df_aligned %>%
    filter(between(case_completed_at, as.Date("2025-11-16"), as.Date("2025-12-19"))) %>% 
    filter(date(case_completed_at) %in% unique(date(df_cases$case_completed_at)) | date(create_dt_utc) %in% unique(date(df_cases$case_completed_at))) %>% 
  arrange(case_completed_at, create_dt_utc)

# EXPORT ------------------------------------------------------------------

  write_csv("Dataout/20251201_la_case-numbers_confirmation-codes.csv", na = "")
