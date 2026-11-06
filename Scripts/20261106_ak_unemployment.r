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

path_ak <- "Data/Regional_Monthly_Values_1990_and_Up.csv"

path_us <- "Data/UNRATENSA.csv"


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
  "Skagway Municipality",
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
  "Kenai Peninsula Borough",
  "Kodiak Island Borough",
  "Chugach Census Area",
  "Aleutians West Census Area",
  "Southeast Fairbanks Census Area",
  "Denali Borough",
  "Hoonah-Angoon Census Area",
  "Copper River Census Area",
  "Petersburg Borough",
  "Haines Borough",
  "City and Borough of Wrangell",
  "Lake and Peninsula Borough",
  "Bristol Bay Borough",
  "City and Borough of Yakutat"
)

# IMPORT ------------------------------------------------------------------

#AK seasonally unadjusted unemployment
#source: https://akgeoportal.alaska.gov/datasets/DCCED::alaska-unemployment-rates-by-borough-and-census-area/about
df_ak <- read_csv(
  path_ak,
  name_repair = make_clean_names
)

#National unemployment
#source: https://fred.stlouisfed.org/series/UNRATENSA/
df_us <- read_csv(
  path_us,
  name_repair = make_clean_names
)

# REVIEW DATA ------------------------------------------------------------

#are there missing locale?
# setdiff(unique(df_ak$community_name), v_provided)
# setdiff(v_provided, unique(df_ak$community_name))

# unique(df_ak$community_name) |> sort()

# str_subset(unique(df_ak$community_name), "Anchorage")

# df_ak |>
#   filter(data_year >= 2024, str_detect(community_name, "Anchorage")) |>
#   count(community_name)

# MUNGE -------------------------------------------------------------------

#subset to recent data and a local in the file AK sent over
df_ak <- df_ak |>
  filter(
    data_year >= 2024,
    community_name %in% v_provided
  )

#clean up date for plotting and convert rate to a rate
df_ak <- df_ak |>
  mutate(
    date = make_date(data_year, period),
    unemployment_rate = unemployment_rate / 100
  )


#clean up US unemployment rate, limiting to AK date range
df_us <- df_us |>
  mutate(unratensa = unratensa / 100) |>
  rename(
    unemployment_rate_us = unratensa,
    date = observation_date
  ) |>
  filter(
    date >= min(df_ak$date),
    date <= max(df_ak$date),
  )


#merge US unemployment (not seasonally adjusted) onto AK locale data
df_unempl <- df_ak |>
  bind_rows(
    df_us |>
      mutate(
        community_name = "US",
        data_year = year(date)
      ) |>
      rename(unemployment_rate = unemployment_rate_us)
  ) |>
  left_join(df_us, by = join_by(date))


# capture latest rate for ordering + labeling in plots
df_unempl <- df_unempl |>
  mutate(
    labor_force_latest = case_when(
      community_name == "US" ~ 0,
      date == max(date) ~ labor_force
    ),
  ) |>
  fill(labor_force_latest, .by = community_name, .direction = "updown")


#capture mean unemployment rate for ordering in plots
df_unempl <- df_unempl |>
  group_by(community_name) |>
  mutate(
    unemployment_rate_mean_2025 = case_when(
      data_year == 2025 ~ unemployment_rate
    ),
    unemployment_rate_mean_2025 = mean(
      unemployment_rate_mean_2025,
      na.rm = TRUE
    )
  ) |>
  fill(unemployment_rate_mean_2025, .direction = "updown") |>
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
      community_name == "US" ~ "#f7f4f9",
      TRUE ~ "#e7e1ef"
    )
  )

#label  + groupings for plot (combine name with latest labor force rate)
df_unempl <- df_unempl |>
  mutate(
    locale_group = case_when(
      community_name %in% v_persistent ~ "persistent",
      community_name %in% v_seasonal ~ "seasonal",
      community_name == "US" ~ " ",
      TRUE ~ ""
    ),
    locale_group = factor(locale_group, c("persistent", "seasonal", "", " ")),
    comm_lab = case_when(
      community_name == "US" ~ community_name,
      community_name == "Bethel Census Area" ~ glue(
        "{community_name} ({format(max(df_ak$date), '%b %Y')} Labor Force = {label_comma()(labor_force_latest)})"
      ),
      TRUE ~ glue("{community_name} ({label_comma()(labor_force_latest)})")
    )
  )

#remove 1 case of duplicate
df_unempl <- df_unempl |>
  group_by(community_name, date) |>
  slice_head(n = 1) |>
  ungroup()


#look at how many locales are in each group
df_unempl |>
  filter(
    community_name != "US",
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
    alpha = .1
  ) +
  geom_line(
    aes(date, unemployment_rate_us * 1.5),
    color = "#909090",
    alpha = .6,
    data = df_us |> filter(between(date, min(df_ak$date), max(df_ak$date)))
  ) +
  geom_line(aes(group = comm_lab), color = "#dd1c77") +
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
    Source: Alaska Department of Labor + FRED"
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
    alpha = .1
  ) +
  geom_line(
    aes(date, unemployment_rate_us * 1.5),
    color = "#909090",
    alpha = .6,
    # linewidth = 1.5,
    data = df_us |> filter(between(date, min(df_ak$date), max(df_ak$date)))
  ) +
  geom_line(aes(group = comm_lab), color = "#c994c7") +
  facet_wrap(
    ~ fct_reorder(comm_lab, labor_force_latest, .desc = TRUE, .na_rm = TRUE)
  ) +
  scale_y_continuous(label = label_percent(), limits = c(0, NA)) +
  scale_x_date(date_labels = "%b %Y") +
  labs(
    x = NULL,
    y = NULL,
    title = "Fourteen locales experience high unemployment seasonally" |>
      toupper(),
    subtitle = "High unemployment is 1.5x or more above the US monthly rate (gray line)",
    caption = "Note: Unemployment rate is not seasonally adjusted
    Source: Alaska Department of Labor + FRED"
  ) +
  si_style() +
  theme(panel.spacing = unit(1, "lines"))

# si_preview()
si_save("Images/ak_unempl_locale_trends_seasonal.png")


#heat map of unemployment rates
df_unempl |>
  ggplot(aes(date, fct_reorder(comm_lab, labor_force_latest))) +
  geom_tile(aes(fill = fill_color, color = 'white')) +
  geom_text(
    aes(label = label_percent(.1)(unemployment_rate)),
    size = 6 / .pt,
    family = "Source Sans"
  ) +
  facet_grid(
    locale_group ~ data_year,
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
    subtitle = "7 locales have have persisent high unemployment with another 14 experiencing it seasonally",
    caption = "Note: High unemployment is 1.5x or more above the US monthly rate | Unemployment rate is not seasonally adjusted
    Source: Alaska Department of Labor + FRED"
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
    data_year == max(data_year),
    community_name != "US"
  ) |>
  group_by(community_name, locale_group) |>
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
    x = "Labor Force (2025 Avg, Log Scale)",
    y = "Unemployment Rate (2025 Avg)",
    title = "Seaonal unemployment more typically seen in smaller locales" |>
      toupper(),
    subtitle = "All locales are depicted in gray for context",
    caption = "Note: High unemployment is 1.5x or more above the US monthly rate | Unemployment rate is not seasonally adjusted
    Source: Alaska Department of Labor + FRED"
  ) +
  si_style() +
  theme(legend.position = "none")

# si_preview()
si_save("Images/ak_unempl_avg_scatter.png")
