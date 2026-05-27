# PROJECT:  dsac_misc
# PURPOSE:  review rolling Emmy completenes rate
# AUTHOR:   A.Chafetz | CMS
# REF ID:   e27a715af908
# LICENSE:  MIT
# DATE:     2026-05-26
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


# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "e27a715af908" #a reference to be placed in viz captions

start_date <- pilot_pds |>
  slice_tail(n = 1) |>
  pull(start_date)

bounding_events <- c(
  "CaseworkerInvitedApplicantToFlow",
  "ApplicantClickedCBVInvitationLink",
  "ApplicantViewedAgreement",
  "ApplicantSharedIncomeSummary"
)

# IMPORT ------------------------------------------------------------------

library(tidyverse)
library(patchwork)
library(ggtext)
library(scales)
library(emmytics)
library(systemfonts, warn.conflicts = FALSE)


# MUNGE -------------------------------------------------------------------

df <- get_mixpanel_data(
  start_date,
  today(),
  "nh_dhhs",
  events = bounding_events
)

df <- df |>
  extract_properties(
    seconds_since_invitation = properties$seconds_since_invitation
  )

df_bene <- df |>
  filter(event != "CaseworkerInvitedApplicantToFlow")


df_bene <- df_bene |>
  group_by(distinct_id, event) |>
  filter(timestamp == min(timestamp, na.rm = TRUE)) |>
  ungroup()

df_bene <- df_bene |>
  mutate(
    event = ifelse(
      event == "ApplicantClickedCBVInvitationLink",
      "ApplicantInvitatedToFlow",
      event
    ),
    seconds_since_invitation = ifelse(
      is.na(seconds_since_invitation),
      0,
      seconds_since_invitation
    ),
    timestamp = timestamp - seconds(seconds_since_invitation),
    date = as.Date(timestamp)
  )

df_bene <- df_bene |>
  distinct(distinct_id, date, event) |>
  pivot_wider(
    names_from = event,
    values_from = date
  )

df_bene <- df_bene |>
  filter(!is.na(ApplicantInvitatedToFlow)) |>
  group_by(invite_date = ApplicantInvitatedToFlow) |>
  summarise(
    applicants_started = n(),
    applicants_completed = sum(
      !is.na(ApplicantSharedIncomeSummary),
      na.rm = TRUE
    ),
    completion_rate = applicants_completed / applicants_started,
    .groups = "drop"
  )

df_cw <- df |>
  filter(event == "CaseworkerInvitedApplicantToFlow") |>
  mutate(invite_date = as.Date(timestamp)) |>
  count(invite_date, name = "caseworker_invites")

df_trend <- full_join(df_cw, df_bene, by = join_by(invite_date))

df_viz <- df_trend |>
  mutate(complete = max(invite_date) - invite_date >= 10) |>
  group_by(complete) |>
  mutate(
    cum_rate = cumsum(applicants_completed) / cumsum(applicants_started),
    pd_rate = sum(applicants_completed, na.rm = TRUE) /
      sum(applicants_started, na.rm = TRUE),
    lab_rate = case_when(invite_date == max(invite_date) ~ cum_rate)
  ) |>
  ungroup() |>
  mutate(
    pd_rate = case_when(complete == TRUE ~ pd_rate),
    cum_rate = case_when(complete == TRUE ~ cum_rate),
    lab_rate = case_when(complete == TRUE ~ lab_rate),
  ) |>
  mutate(fill_color = ifelse(complete, dsac_light_navy, "#808080"))

# VIZ ---------------------------------------------------------------------

df_viz |>
  ggplot(aes(invite_date, completion_rate, fill = fill_color)) +
  geom_col(alpha = .8) +
  geom_step(
    aes(y = cum_rate),
    na.rm = TRUE,
    position = position_nudge(x = 1 / 2),
    direction = "vh",
    linewidth = 1.2,
    color = dsac_navy,
    lineend = "round"
  ) +
  geom_point(
    aes(x = invite_date + .4, y = lab_rate),
    na.rm = TRUE,
    color = dsac_navy,
    size = 3
  ) +
  geom_label(
    aes(y = lab_rate, label = label_percent(.1)(lab_rate)),
    na.rm = TRUE,
    size = 14 / .pt,
    # vjust = 2,
    hjust = -.25,
    fill = "white",
    family = "Source Sans 3 SemiBold",
    color = dsac_navy,
    linewidth = 1.1
  ) +
  scale_x_date(date_labels = "%b %d") +
  scale_y_continuous(label = label_percent()) +
  labs(
    x = NULL,
    y = NULL,
    title = "Completion Rate within 10 days of receiving notice" |> toupper(),
    subtitle = "Cumulative completion rate by invitation cohort (date sent)",
    caption = str_glue(
      "Gray bars reflect cohorts sent invitation less than 10 days ago
    Source: Emmy Analytics [pulled {today()}] | Ref id: {ref_id}"
    )
  ) +
  scale_fill_identity() +
  si_style_ygrid()

si_preview()

df_viz |>
  ggplot(aes(caseworker_invites, completion_rate)) +
  geom_smooth(method = "lm", se = FALSE, alpha = .3) +
  geom_point(size = 3, alpha = .6) +
  scale_x_log10() +
  si_style()
