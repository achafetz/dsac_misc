# PROJECT:  iv-cbv-payroll
# AUTHOR:   A.Chafetz | CMS
# REF ID:   b5968b99
# PURPOSE:  review sync times
# LICENSE:  MIT
# DATE:     2026-03-11
# UPDATED:

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(arrow, warn.conflicts = FALSE)
library(glitr)
library(gt)
library(gtExtras)
library(systemfonts)
library(emmytics)

# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "b5968b99" #a reference to be places in viz captions

#default viz/table caption
default_caption <- str_glue(
    "Source: 2025 EMMY Pilot Mixpanel Data [accessed 2025-12-19] | Ref id: {ref_id}"
)

#data path
fldr_path <- "../iv-cbv-payroll/app/analytics/"

#identify paths for json data for each of the three periods
mp_path <- list.files(
    fldr_path,
    "json",
    full.names = TRUE
)


# IMPORT ------------------------------------------------------------------

df_mp <- mp_path |>
    set_names() |>
    map(~ read_mixpanel(.x, drop_prop = FALSE)) |>
    list_rbind(names_to = "source_path")


# MUNGE -------------------------------------------------------------------

#limit to just sync events
df_syncs <- df_mp |>
    filter(event == "ApplicantFinishedSync")

#remove duplicate records coming in from pull (eg AZ data coming in for the LA pull)
df_syncs <- df_syncs |>
    mutate(
        source_path = basename(source_path),
        start_date = str_extract(source_path, "\\d{4}-\\d{2}-\\d{2}") |>
            as_date(),
        end_date = str_extract(source_path, "(?<=_to_)\\d{4}-\\d{2}-\\d{2}") |>
            as_date()
    ) |>
    semi_join(
        pilot_pds,
        by = join_by(start_date, end_date, "pilot_state" == "state")
    ) |>
    select(-c(source_path, start_date, end_date))

# df_mp |>
#   distinct(source_path, pilot_state)

#extract sync times from properties for analysis
df_syncs <- df_syncs |>
    extract_properties(sync_duration_seconds = properties$sync_duration_seconds)

#combine pilot state and pds together for table labeling
df_syncs <- df_syncs |>
    unite(pilot, c('pilot_state', 'pilot'), sep = " ")

# add overall and provider roll ups
df_syncs_stats <- df_syncs |>
    bind_rows(
        df_syncs |> mutate(pilot = "Overall")
    ) |>
    bind_rows(
        df_syncs |> mutate(pilot = provider)
    )

#create groups and convert pilots to factor for ordering in table
df_syncs_stats <- df_syncs_stats |>
    mutate(
        group = case_when(
            pilot == "Overall" ~ "",
            pilot %in% c("Argyle", "Pinwheel") ~ "Provider",
            TRUE ~ "Pilot"
        ),
        group = factor(group, c("", "Provider", "Pilot")),
        pilot = factor(
            pilot,
            c(
                "Overall",
                "Argyle",
                "Pinwheel",
                "AZ Jun 2025",
                "AZ Aug 2025",
                "LA May 2025",
                "LA Aug 2025",
                "LA Nov 2025"
            )
        )
    )

#summary stats to present
df_syncs_stats <- df_syncs_stats |>
    group_by(group, pilot) |>
    summarise(
        n = n(),
        min = min(sync_duration_seconds, na.rm = TRUE),
        q25 = quantile(sync_duration_seconds, 0.25, na.rm = TRUE),
        median = median(sync_duration_seconds, na.rm = TRUE),
        mean = mean(sync_duration_seconds, na.rm = TRUE),
        q75 = quantile(sync_duration_seconds, 0.75, na.rm = TRUE),
        q95 = quantile(sync_duration_seconds, 0.95, na.rm = TRUE),
        max = max(sync_duration_seconds, na.rm = TRUE),
        .groups = "drop"
    ) |>
    arrange(group, pilot)


# VIZ ---------------------------------------------------------------------

df_syncs_stats |>
    gt(groupname_col = "group") |>
    cols_hide(group) |>
    cols_label("pilot" = "") |>
    fmt_number(
        decimals = 0
    ) |>
    # gt_highlight_rows(
    #   rows = 1,
    #   fill = dsac_light_teal
    # ) |>
    cols_align(
        columns = pilot,
        align = "left"
    ) |>

    tab_spanner(
        label = "summary stats (in seconds)",
        columns = 4:9
    ) |>
    tab_options(
        data_row.padding = px(3),
        summary_row.padding = px(3),
        row_group.padding = px(5)
    ) |>
    sub_missing(missing_text = "") |>
    opt_stylize(style = 6, color = 'gray') |>
    tab_header(
        title = "EMMY PILOT SYNC TIMES"
    ) |>
    tab_source_note(default_caption)
