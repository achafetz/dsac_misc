# PROJECT:  dsac_misc
# PURPOSE:  LDH fallout case alignment
# AUTHOR:   A.Chafetz | CMS
# REF ID:   3536c7b6c12c
# LICENSE:  MIT
# DATE:     2026-03-12
# UPDATED:

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(glue)
library(gagglr) ##install.packages('gagglr', repos = c('https://usaid-oha-si.r-universe.dev', 'https://cloud.r-project.org'))
library(scales, warn.conflicts = FALSE)
library(systemfonts)
library(tidytext)
library(patchwork)
library(ggtext)


# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "3536c7b6c12c" #a reference to be placed in viz captions


# IMPORT ------------------------------------------------------------------

df_mp <- read_mixpanel(
  "Data/mixpanel_data_la_ldh_2026-02-14_to_2026-03-10.json",
  employer_name = properties$employment_employer_name,
  query = properties$query
)

#fallout dataset for March 2 week
df_fallout <- read_csv(
  "Data/3.6 CBV fallout reasons.csv",
  skip = 4
)


# MUNGE -------------------------------------------------------------------

#clean up names
df_fallout <- df_fallout |>
  rename(
    "ldh_notes1" = ...5,
    "ldh_notes2" = ...6,
    fallout_reason = `Fallout Reason`
  )

#covert to LA timezones from UTC and flag the files that were processed
df_fallout <- df_fallout |>
  mutate(
    across(ends_with("_at"), \(x) as_datetime(x)) |> with_tz("America/Chicago"),
    ldh_processed = TRUE
  )

#print out the number of records in fallout report
nrow(df_fallout)

#subset the mixpanel data to submissions and those falling between the period from the report
df_lim <- df_mp |>
  filter(
    event == "ApplicantSharedIncomeSummary",
    between(
      timestamp,
      as_date(min(df_fallout$transmitted_at)) + 1,
      as_date(max(df_fallout$transmitted_at)) + 1
    )
  ) |>
  select(
    timestamp,
    emmy_distinct_id = distinct_id,
    emmy_cbv_flow_id = cbv_flow_id
  ) |>
  mutate(emmy_processed = TRUE)

#print out the number of records from Mixpanel
nrow(df_lim)

#review mismatches
df_lim |>
  anti_join(df_fallout, by = c('timestamp' = 'transmitted_at')) |>
  glimpse()

df_fallout |>
  anti_join(df_lim, by = c('transmitted_at' = 'timestamp')) |>
  glimpse()

#join data
df_join <- df_fallout |>
  select(-c(started_at, completed_at)) |>
  full_join(df_lim, by = c('transmitted_at' = 'timestamp'))

#apply status + tidy
df_join <- df_join |>
  mutate(
    status = case_when(
      !is.na(fallout_reason) ~ "Fallout",
      is.na(ldh_processed) ~ "Missing from LDH",
      TRUE ~ "Processed"
    )
  ) |>
  relocate(
    status,
    fallout_reason,
    case_number,
    emmy_distinct_id,
    emmy_cbv_flow_id,
    .before = 1
  ) |>
  select(-ends_with("processed")) |>
  arrange(status, transmitted_at) |>
  mutate(transmitted_at = with_tz(transmitted_at, "UTC"))


#review
v_missing <- df_join |>
  filter(status == "Missing from LDH") |>
  pull(emmy_distinct_id)

df_mp |>
  follow_applicant(v_missing[1])


# EXPORT -----------------------------------------------------------------

df_join |>
  write_csv("Dataout/2026-feb-cycle_ldh_fallout-review.csv", na = "")
