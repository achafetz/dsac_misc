# PROJECT:  dasc_misc
# PURPOSE:  verification + cost projects
# AUTHOR:   A.Chafetz | CMS
# REF ID:   46679c279688
# LICENSE:  MIT
# DATE:     2026-09-30
# UPDATED:

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(glue)
library(emmytics)
library(gagglr) ##install.packages('gagglr', repos = c('https://usaid-oha-si.r-universe.dev', 'https://cloud.r-project.org'))
library(scales, warn.conflicts = FALSE)
library(systemfonts)
library(tidytext)
library(patchwork)
library(ggtext)
library(gt)
library(gtayblr)

# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "46679c279688" #a reference to be placed in viz captions

#identify start data for latest rolling pilot
df_start <- pilot_pds |>
  filter(state %in% c("LA", "NH")) |>
  group_by(state) |>
  slice_tail() |>
  select(state, start_date)

#Price of verification from Argyle-CMS contract
vprice <- 0 #need to add actually price locally for analysis

# IMPORT ------------------------------------------------------------------

df_mp <- get_mixpanel_data(
  from_date = "2025-10-01",
  to_date = "2026-09-30",
  event = c("ApplicantFinishedArgyleSync", "ApplicantFinishedPinwheelSync")
)

# MUNGE -------------------------------------------------------------------

#include month for aggregation purposes
df_syncs <- df_mp |>
  clean_events() |>
  mutate(month = floor_date(timestamp, unit = "month") |> as.Date())

#what share of synces are from pinwheel
df_syncs |>
  count(provider) |>
  mutate(share = n / sum(n))

#what share of synces are from pinwheel by pilot
df_syncs |>
  count(event_clean, pilot_state, provider) |>
  group_by(pilot_state) |>
  mutate(share = n / sum(n))

#aggregate to monthly
df_syncs_month <- df_syncs |>
  count(event_clean, pilot_state, month)

#identify full months for rolling pilots to calc summary stats over
df_syncs_month <- df_syncs_month |>
  left_join(
    df_start |> mutate(start_date = ceiling_date(start_date, unit = "month")),
    by = join_by(
      x$pilot_state == y$state,
      x$month >= y$start_date
    )
  ) |>
  group_by(pilot_state, start_date) |>
  mutate(
    mean = mean(n) |> round(),
    median = median(n),
    sd = sd(n) |> round()
  ) |>
  ungroup()

#clean up for viz
df_syncs_month <- df_syncs_month |>
  mutate(
    pilot_state = recode_values(
      pilot_state,
      "LA" ~ "Louisiana",
      "NH" ~ "New Hampshire"
    ),
    across(c(mean, median, sd), ~ case_when(!is.na(start_date) ~ .x)),
    fill_color = ifelse(is.na(mean), "#909090", dsac_teal)
  )

# VIZ ---------------------------------------------------------------------

df_syncs_month |>
  ggplot(aes(month, n, fill = fill_color)) +
  geom_ribbon(
    aes(ymin = mean - sd, ymax = mean + sd),
    alpha = .2,
    na.rm = TRUE
  ) +
  geom_col() +
  geom_line(aes(y = mean), na.rm = TRUE, linetype = "dashed") +
  facet_wrap(~pilot_state) +
  labs(
    x = NULL,
    y = NULL,
    title = "New Hampshire has a more consistent monthly utilization each month" |>
      toupper(),
    subtitle = "Both states see an uptick in applicant syncs with payroll provider aggregate every three months",
    caption = "Note: Includes both Argyle (>90%) and Pinwheel  | Event = 'ApplicantFinished*Sync' | Green fill represents full months when pilots was rolling admission where mean was calculated
     Source: Emmy Mixpanel [2026-09-30]"
  ) +
  scale_fill_identity() +
  scale_y_continuous(label = label_comma()) +
  si_style()

#export
si_save("Graphics/syncs.svg")


# TABLE ------------------------------------------------------------------

#setup table to breakdown monthly verifications + costs
df_syncs_breakdown <- df_syncs_month |>
  filter(!is.na(mean)) |>
  group_by(pilot_state) |>
  slice_head() |>
  ungroup() |>
  select(pilot_state, monthly.verifications = mean) |>
  mutate(
    annual.verifications = monthly.verifications * 12,
    monthly.cost = monthly.verifications * vprice,
    monthly.cost_cms = monthly.cost * .75,
    annual.cost = annual.verifications * vprice,
    annual.cost_cms = annual.cost * .75,
  ) |>
  pivot_longer(
    cols = -pilot_state,
    names_to = c("period", ".value"),
    names_pattern = "^(monthly|annual)\\.(.+)$"
  )


tbl <- df_syncs_breakdown |>
  gt(
    groupname_col = "pilot_state",
    rowname_col = "period"
  ) |>
  cols_label(
    verifications = "Verifications",
    cost = "Total Cost",
    cost_cms = "CMS Cost"
  ) |>
  fmt_currency(
    columns = c(cost, cost_cms),
    decimals = 1,
    suffixing = TRUE,
    use_subunits = FALSE
  ) |>
  fmt_number(
    columns = verifications,
    decimals = 0,
    suffixing = TRUE
  ) |>
  tab_source_note(
    source_note = md(
      "Note: Verifications based on monthly mean from pilot |
    Source: Emmy Mixpanel [2026-09-30]"
    )
  ) |>
  gtayblr::si_gt_base()

#export
gtsave(
  tbl,
  "Images/table.png"
)
