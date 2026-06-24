# PROJECT:  dsac_misc
# PURPOSE:  pulling stats for Emmy A/B testing
# AUTHOR:   A.Chafetz | CMS
# REF ID:   fbc9f50f4645
# LICENSE:  MIT
# DATE:     2026-06-24
# UPDATED:

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(emmytics)
library(gt)


# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "fbc9f50f4645" #a reference to be placed in viz captions

start <- pilot_pds |>
  filter(state == "LA") |>
  slice_tail() |>
  pull(start_date)

bounding_events <-
  c(
    "ApplicantViewedAgreement",
    "ApplicantAgreed",
    "ApplicantSharedIncomeSummary"
  )

# IMPORT ------------------------------------------------------------------

df_mp <- get_mixpanel_data(
  from_date = start,
  to_date = today(),
  events = bounding_events
)

# MUNGE -------------------------------------------------------------------

#Average sessions per day for LA
df_sess <- df_mp |>
  filter(event == "ApplicantViewedAgreement") |>
  mutate(
    date = as_date(timestamp),
    units = "sessions",
  ) |>
  group_by(pilot_state, units, date) |>
  summarise(
    n_distinct = n_distinct(cbv_flow_id),
    .groups = "drop"
  ) |>
  group_by(pilot_state, units) |>
  summarise(
    pd_start = min(date),
    pd_end = max(date),
    avg_daily = mean(n_distinct) |> round(),
    median_daily = median(n_distinct),
    .groups = "drop"
  ) |>
  relocate(units, .after = pd_end)

#Average users per day for LA
df_user <- df_mp |>
  filter(event == "ApplicantViewedAgreement") |>
  mutate(
    date = as_date(timestamp),
    units = "users",
  ) |>
  group_by(pilot_state, units, date) |>
  summarise(
    n_distinct = n_distinct(distinct_id),
    .groups = "drop"
  ) |>
  group_by(pilot_state, units) |>
  summarise(
    pd_start = min(date),
    pd_end = max(date),
    avg_daily = mean(n_distinct) |> round(),
    median_daily = median(n_distinct),
    .groups = "drop"
  ) |>
  relocate(units, .after = pd_end)


#Average/Estimate of conversion rate off of home page
df_sess_compl <- df_mp |>
  # filter(as.Date(timestamp) <= today() - days(10)) |>
  distinct(pilot_state, cbv_flow_id, event) |>
  mutate(
    event = recode_values(
      event,
      "ApplicantViewedAgreement" ~ "total_opened",
      "ApplicantAgreed" ~ "total_started",
      "ApplicantSharedIncomeSummary" ~ "total_finished",
    )
  ) |>
  count(pilot_state, event) |>
  pivot_wider(
    names_from = event,
    values_from = n
  ) |>
  relocate(total_opened, total_started, total_finished, .after = pilot_state) |>
  mutate(completion_rate = total_finished / total_started)

df_user_compl <- df_mp |>
  # filter(as.Date(timestamp) <= today() - days(10)) |>
  distinct(pilot_state, distinct_id, event) |>
  mutate(
    event = recode_values(
      event,
      "ApplicantViewedAgreement" ~ "total_opened",
      "ApplicantAgreed" ~ "total_started",
      "ApplicantSharedIncomeSummary" ~ "total_finished",
    )
  ) |>
  count(pilot_state, event) |>
  pivot_wider(
    names_from = event,
    values_from = n
  ) |>
  relocate(total_opened, total_started, total_finished, .after = pilot_state) |>
  mutate(completion_rate = total_finished / total_started)


#merge

df_tbl <- bind_rows(
  left_join(
    df_sess,
    df_sess_compl,
    by = join_by(pilot_state),
  ),
  left_join(
    df_user,
    df_user_compl,
    by = join_by(pilot_state),
  ),
)


# VIZ ---------------------------------------------------------------------

v_pd <- df_tbl |>
  distinct(pilot_state, pd_start, pd_end) |>
  mutate(note = str_glue("{pilot_state}: {pd_start} - {pd_end}")) |>
  pull() |>
  paste(collapse = "; ")

df_tbl |>
  select(-starts_with("pd")) |>
  mutate(units = str_to_sentence(units)) |>
  gt(
    # groupname_col = "units",
    # rowname_col = "pilot_state"
    groupname_col = "pilot_state",
    rowname_col = "units"
  ) |>
  cols_label(
    avg_daily = "Mean",
    median_daily = "Median",
    total_opened = "Opened",
    total_started = "Started",
    total_finished = "Finished",
    completion_rate = "Rate"
  ) |>
  tab_spanner(
    label = "Daily Counts",
    columns = ends_with("daily")
  ) |>
  tab_spanner(
    label = "Overall",
    columns = !ends_with("daily")
  ) |>
  fmt_auto() |>
  fmt_percent(
    columns = "completion_rate",
    decimals = 1
  ) |>
  cols_align(align = "right") |>
  cols_width(everything() ~ px(80)) |>
  tab_source_note(str_glue("Emmy Mixpanel Data | ", "Periods = {v_pd} ")) |>
  gtsave("Images/ab_info_20260624.png")
