# PROJECT:  dsac_misc
# PURPOSE:  help provide justification for hardship waiver (for unemployment)
# AUTHOR:   A.Chafetz | CMS
# REF ID:
# LICENSE:  MIT
# DATE:     2026-11-03
# UPDATED:

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(glue)
library(glitr) ##install.packages('glitr', repos = c('https://usaid-oha-si.r-universe.dev', 'https://cloud.r-project.org'))
library(scales, warn.conflicts = FALSE)
library(systemfonts)
library(tidytext)
library(patchwork)
library(ggtext)
library(janitor)
library(ggrepel)


# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "773c73732db8" #a reference to be placed in viz captions

path_ak <- "Data/Alaska_NOT_SEASONALLY_adjusted.csv"

path_us <- "Data/United States_NOT_SEASONALLY_adjusted.csv"


#list of locales from file AK sent over
v_provided <- c(
  "Aleutians East Borough",
  "Aleutians West Census Area",
  "Municipality of Anchorage",
  "Bethel Census Area",
  "Bristol Bay Borough",
  "Chugach Census Area",
  "Copper River Census Area",
  "Denali Borough",
  "Dillingham Census Area",
  "Haines Borough",
  "Hoonah-Angoon Census Area",
  "Kenai Peninsula Borough",
  "Kodiak Island Borough",
  "Kusilvak Census Area",
  "Lake and Peninsula Borough",
  "Matanuska-Susitna Borough",
  "Nome Census Area",
  "North Slope Borough",
  "Northwest Arctic Borough",
  "Petersburg Borough",
  "Prince of Wales-Hyder Census Area",
  "City and Borough of Sitka",
  "Municipality of Skagway",
  "Southeast Fairbanks Census Area",
  "City and Borough of Wrangell",
  "City and Borough of Yakutat",
  "Yukon-Koyukuk Census Area",
  "City and Borough of Juneau",
  "Ketchikan Gateway Borough",
  "Anchorage/Mat-Su MSA"
)

#locales with persistently high unemployment (from reviewing heatmap)
v_persistent <- c(
  "Bethel Census Area",
  "Nome Census Area",
  "Northwest Arctic Borough",
  "Yukon-Koyukuk Census Area",
  "Prince of Wales-Hyder Census Area",
  "Kusilvak Census Area",
  "Dillingham Census Area"
)


#locales with seasonal high unemployment (from reviewing heatmap)
v_seasonal <- c(
  "Kodiak Island Borough",
  "Chugach Census Area",
  "Southeast Fairbanks Census Area",
  "Denali Borough",
  "Hoonah-Angoon Census Area",
  "Copper River Census Area",
  "Petersburg Borough",
  "Haines Borough",
  # "City and Borough of Wrangell",
  "Lake and Peninsula Borough",
  "Bristol Bay Borough",
  "City and Borough of Yakutat",
  "Municipality of Skagway",
  # "Aleutians West Census Area",
  "Aleutians East Borough"
)

# IMPORT ------------------------------------------------------------------

#source: https://live.laborstats.alaska.gov/data-pages/labor-force-area-data?a=0&s=1
#AK seasonally unadjusted unemployment
df_ak <- read_csv(
  path_ak,
  skip = 1,
  name_repair = make_clean_names
)

#National unemployment
df_us <- read_csv(
  path_us,
  skip = 1,
  name_repair = make_clean_names
)

# REVIEW DATA ------------------------------------------------------------

#are there missing locale?
# setdiff(unique(df_ak$area_name), v_provided)
# setdiff(v_provided, unique(df_ak$area_name))

# unique(df_ak$area_name) |> sort()

# str_subset(unique(df_ak$area_name), "Anchorage")

# df_ak |>
#   filter(year >= 2024, str_detect(area_name, "Anc")) |>
#   count(area_name)

# MUNGE -------------------------------------------------------------------

#subset to recent data & clean up date for plotting and convert rate to a rate
df_ak <- df_ak |>
  filter(
    year >= 2024,
    area_name %in% v_provided,
    month != "Annual"
  ) |>
  mutate(
    date = make_date(year, period),
    unemployment_rate = unemployment_rate / 100
  )


#clean up US unemployment rate, limiting to AK date range
df_us <- df_us |>
  filter(
    year >= min(df_ak$year),
    month != "Annual"
  ) |>
  mutate(
    date = make_date(year, period),
    unemployment_rate = unemployment_rate / 100,
    area_name = "US"
  )


#merge US unemployment (not seasonally adjusted) onto AK locale data
df_unempl <- df_ak |>
  filter(date <= max(df_us$date)) |>
  bind_rows(df_us) |>
  left_join(
    df_us |>
      select(date, unemployment_rate_us = unemployment_rate),
    by = join_by(date)
  )


# capture latest rate for ordering + labeling in plots
df_unempl <- df_unempl |>
  group_by(area_name) |>
  mutate(
    labor_force_latest = case_when(
      area_name == "US" ~ 0,
      date == max(date, na.rm = TRUE) ~ labor_force
    ),
  ) |>
  fill(labor_force_latest, .direction = "updown") |>
  ungroup()


#capture mean unemployment rate for ordering in plots
df_unempl <- df_unempl |>
  group_by(area_name) |>
  mutate(
    unemployment_rate_latest_yr = case_when(
      year == max(year) ~ unemployment_rate
    ),
    unemployment_rate_latest_yr = mean(
      unemployment_rate_latest_yr,
      na.rm = TRUE
    )
  ) |>
  fill(unemployment_rate_latest_yr, .direction = "updown") |>
  ungroup()

#identify high unemployment locales
df_unempl <- df_unempl |>
  mutate(
    flag = case_when(
      unemployment_rate >= .08 ~ "v high",
      unemployment_rate >= 1.5 * unemployment_rate_us ~ "high",
    ),
    fill_color = case_when(
      flag == "v high" ~ "#dd1c77",
      flag == "high" ~ "#c994c7",
      area_name == "US" ~ "#f7f4f9",
      TRUE ~ "#e7e1ef"
    )
  )

#label  + groupings for plot (combine name with latest labor force rate)
df_unempl <- df_unempl |>
  mutate(
    locale_group = case_when(
      area_name %in% v_persistent ~ "persistent",
      area_name %in% v_seasonal ~ "seasonal",
      area_name == "US" ~ " ",
      TRUE ~ ""
    ),
    locale_group = factor(locale_group, c("persistent", "seasonal", "", " ")),
    comm_lab = case_when(
      area_name == "US" ~ area_name,
      area_name == "Bethel Census Area" ~ glue(
        "{area_name} ({format(max(df_ak$date), '%b %Y')} Labor Force = {label_comma()(labor_force_latest)})"
      ),
      TRUE ~ glue("{area_name} ({label_comma()(labor_force_latest)})")
    )
  )

#remove 1 case of duplicate
# df_unempl <- df_unempl |>
#   group_by(area_name, date) |>
#   slice_head(n = 1) |>
#   ungroup()

#look at how many locales are in each group
df_unempl |>
  filter(
    area_name != "US",
    date == max(date)
  ) |>
  count(locale_group) |>
  mutate(share = n / sum(n))

# VIZ ---------------------------------------------------------------------

#trend for persistent unemployment rate
df_unempl |>
  filter(locale_group == "persistent") |>
  ggplot(aes(date, unemployment_rate)) +
  geom_ribbon(
    aes(ymin = unemployment_rate_us * 1.5, ymax = unemployment_rate),
    fill = "#dd1c77",
    alpha = .1,
    na.rm = TRUE
  ) +
  geom_line(
    aes(date, unemployment_rate_us * 1.5),
    color = "#909090",
    alpha = .6,
    na.rm = TRUE
  ) +
  geom_line(aes(group = comm_lab), color = "#dd1c77", na.rm = TRUE) +
  facet_wrap(
    ~ fct_reorder(comm_lab, labor_force_latest, .desc = TRUE, .na_rm = TRUE)
  ) +
  scale_y_continuous(label = label_percent(), limits = c(0, NA)) +
  scale_x_date(date_labels = "%b %Y") +
  labs(
    x = NULL,
    y = NULL,
    title = "Seven locales see persisently high unemeployment" |> toupper(),
    subtitle = "High unemployment is 1.5x or more above the US monthly rate (gray line)",
    caption = "Note: Unemployment rate not seasonally adjusted
    Source: Alaska Department of Labor and Workforce Development"
  ) +
  si_style() +
  theme(panel.spacing = unit(1, "lines"))

# si_preview()
si_save("Images/ak_unempl_locale_trends_persistent.png")

#trend for seasonable unemployment rate
df_unempl |>
  filter(locale_group == "seasonal") |>
  ggplot(aes(date, unemployment_rate)) +
  geom_ribbon(
    aes(ymin = unemployment_rate_us * 1.5, ymax = unemployment_rate),
    fill = "#c994c7",
    alpha = .1,
    na.rm = TRUE
  ) +
  geom_line(
    aes(date, unemployment_rate_us * 1.5),
    color = "#909090",
    alpha = .6,
    # linewidth = 1.5,
    na.rm = TRUE
  ) +
  geom_line(aes(group = comm_lab), color = "#c994c7", na.rm = TRUE) +
  facet_wrap(
    ~ fct_reorder(comm_lab, labor_force_latest, .desc = TRUE, .na_rm = TRUE)
  ) +
  scale_y_continuous(label = label_percent(), limits = c(0, NA)) +
  scale_x_date(date_labels = "%b %Y") +
  labs(
    x = NULL,
    y = NULL,
    title = "Thirteen locales experience high unemployment seasonally" |>
      toupper(),
    subtitle = "High unemployment is 1.5x or more above the US monthly rate (gray line)",
    caption = "Note: Unemployment rate is not seasonally adjusted
    Source: Alaska Department of Labor and Workforce Development"
  ) +
  si_style() +
  theme(panel.spacing = unit(1, "lines"))

# si_preview()
si_save("Images/ak_unempl_locale_trends_seasonal.png")


#heat map of unemployment rates
df_unempl |>
  ggplot(aes(date, fct_reorder(comm_lab, labor_force_latest, .na_rm = TRUE))) +
  geom_tile(aes(fill = fill_color, color = 'white'), na.rm = TRUE) +
  geom_text(
    aes(label = label_percent(.1)(unemployment_rate)),
    size = 5 / .pt,
    family = "Source Sans",
    na.rm = TRUE
  ) +
  facet_grid(
    locale_group ~ year,
    scales = "free",
    space = "free",
    switch = "y"
  ) +
  scale_color_identity() +
  scale_fill_identity(
    guide = "legend",
    labels = c(">= 1.5x US unemployment rate", ">= 8%", "< 1.5x", "National")
  ) +
  scale_x_datetime(
    date_breaks = "months",
    date_labels = "%b",
    expand = c(.005, .005),
    position = "top"
  ) +
  scale_y_discrete(expand = c(.005, .005)) +
  labs(
    x = NULL,
    y = NULL,
    fill = NULL,
    title = "Nearly 75% of Alaska Locales experience persistent or seasonal unemployment" |>
      toupper(),
    subtitle = "7 locales have have persisent high unemployment with another 13 experiencing it seasonally",
    caption = "Note: High unemployment is 1.5x or more above the US monthly rate | Unemployment rate is not seasonally adjusted
    Source: Alaska Department of Labor and Workforce Development"
  ) +
  si_style_nolines() +
  theme(
    panel.spacing = unit(.5, "lines"),
    strip.placement = "outside",
    axis.text = element_text(size = 6),
    strip.text.y = element_text(hjust = .5, size = 8),
    plot.caption = element_text(size = 6),
    legend.key.size = unit(.25, 'lines'),
    legend.text = element_text(size = 8)
  )

# si_preview()
si_save("Images/ak_unempl_locale_heatmap.png")

#setup data for scatter
df_unempl_scatter <- df_unempl |>
  filter(
    year == max(year),
    area_name != "US"
  ) |>
  group_by(area_name, locale_group) |>
  summarise(
    unemployment_rate = mean(unemployment_rate),
    labor_force = mean(labor_force),
    labor_force_latest = max(labor_force_latest),
    .groups = "drop"
  ) |>
  mutate(
    fill_color = recode_values(
      locale_group,
      "persistent" ~ "#dd1c77",
      "seasonal" ~ "#c994c7"
    ),
    locale_group = str_to_sentence(locale_group)
  )

#scatter plot of unemployment + labor rate for context
df_unempl_scatter |>
  filter(locale_group != "") |>
  ggplot(aes(labor_force, unemployment_rate)) +
  geom_point(
    data = df_unempl_scatter |> select(-locale_group),
    color = "grey80",
    size = 3
  ) +
  geom_point(aes(fill = fill_color), shape = 21, color = "white", size = 4) +
  scale_fill_identity() +
  facet_grid(~ fct_rev(locale_group)) +
  scale_x_log10(label = label_number(scale_cut = cut_short_scale())) +
  scale_y_continuous(label = label_percent()) +
  labs(
    x = "Labor Force (2026 Avg, Log Scale)",
    y = "Unemployment Rate (2026 Avg)",
    title = "Seaonal unemployment more typically seen in smaller locales" |>
      toupper(),
    subtitle = "All locales are depicted in gray for context",
    caption = "Note: High unemployment is 1.5x or more above the US monthly rate | Unemployment rate is not seasonally adjusted
    Source: Alaska Department of Labor and Workforce Development"
  ) +
  si_style() +
  theme(legend.position = "none")

# si_preview()
si_save("Images/ak_unempl_avg_scatter.png")
