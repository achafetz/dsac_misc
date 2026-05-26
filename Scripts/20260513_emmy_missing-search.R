# PROJECT:  dsac_misc
# PURPOSE:  review missing search results
# AUTHOR:   A.Chafetz | CMS
# REF ID:   2c11e4445e3f
# LICENSE:  MIT
# DATE:     2026-05-26
# UPDATED:

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(emmytics)
library(tidytext)
library(stringdist)
library(refinr)
library(glitr)
library(scales)


# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "2c11e4445e3f" #a reference to be placed in viz captions

# path <- "Data/mixpanel_data_nh_dhhs_2026-04-27_to_2026-05-12.json"
path <- "Data/mixpanel_data_la_ldh_2026-03-23_to_2026-05-12.json"


state <- path |> str_extract("(?<=mixpanel_data_)[a-z]{2}") |> toupper()
period <- path |>
  str_extract("\\d{4}-\\d{2}-\\d{2}_to_\\d{4}-\\d{2}-\\d{2}") |>
  str_replace("_to_", " - ")


# IMPORT ------------------------------------------------------------------

#import data
df_mp <-
  read_mixpanel(
    path,
    employer_name = properties$employment_employer_name,
    query = properties$query,
    drop_prop = TRUE
  )


# MUNGE -------------------------------------------------------------------

## review users

v_ids <- df_mp |>
  filter(str_detect(event, "Missing")) |>
  slice_sample(n = 10) |>
  pull(distinct_id)

follow_applicant(df_mp, v_ids[1])

df_mp |>
  filter(distinct_id == v_ids[3]) |>
  View()

set.seed(42)
df_mp |>
  slice_sample(n = 10) |>
  select(distinct_id, cbv_flow_id, timestamp)


df_mp |>
  mutate(
    query_clean = query |>
      str_to_lower() |> # already done
      str_trim() |> # remove leading/trailing whitespace
      str_squish() |> # collapse internal whitespace
      str_remove_all("[^a-z0-9 ]") |> # remove punctuation/special chars
      str_replace_all("\\b(inc|llc|ltd|co|corp|the)\\b", "") |> # strip common suffixes
      str_squish() # squish again after removals
  ) |>
  filter(query_clean == "walmart") |>
  # distinct(distinct_id) |>
  # pull() |>
  # clipr::write_clip()
  summarise(
    n_queries = n(),
    users = n_distinct(distinct_id),
    sessions = n_distinct(cbv_flow_id)
  )


# v_ids <- c("device-1ecf4c8c-7945-4e91-8259-1d1946765faa", "device-4505bff7-9d84-432f-bfe3-4abf58558234")
v_ids <- c(
  "device-f4d949bf-cea4-4841-8832-4e7c53c85467",
  "device-faecad5b-4847-4567-9115-a52e8d9858be",
  "device-e21fbb9f-61db-4efb-9da3-b2b19c819641",
  "device-314bcd21-379f-4521-b8a5-667dd81f9b1b",
  "device-9141f7e1-9d00-4c76-8bd2-d9b575c05369",
  "device-00d79b8f-4341-4d73-a9d8-b7cf2915b60e",
  "device-ddce9f2e-3b98-483d-81a1-e191542b5d0f",
  "device-8e5e8a6d-0683-453a-b411-40a931cacd1e",
  "device-f4f5a6c5-50b0-421d-a169-d87b966476b1",
  "device-872bf4b9-9c45-4d68-b9ee-2094ce8031a8",
  "device-d96b4324-d096-45a6-8dc7-ebfa4d125e59",
  "device-3f80e037-e5cd-48ca-b39c-95f6244f016c",
  "device-bcc11bba-5574-4edf-baf2-264cb381c813",
  "device-1ee756d6-0762-4824-91bc-7ac042affb75",
  "device-cb0d63ff-6621-4bff-91fa-ae3099dcdafa",
  "device-bdb1de1d-ce8b-4b20-af65-6f1888f10274",
  "device-e2bbb305-39da-4a10-9381-2cc2a4e10325",
  "device-50665fdb-e86b-471e-9340-825e00aec305",
  "device-20115cc1-a184-43a7-94b5-d4cc0f64c313",
  "device-11e908e9-670a-492e-9f98-277d85dd1c71",
  "device-b73d445a-78b1-4537-bcbb-7efe627f0b55",
  "device-07ee9479-825b-4e61-9d77-2108899e5780",
  "device-7ed91e29-f183-40f1-9271-c666448d939b",
  "device-5b2b4f53-0e5a-4289-9dc2-eb423a14eeec",
  "device-150de073-efce-41f2-b5ea-0e4de7fb99ef",
  "device-8edd0429-fa95-4870-a935-432509382cae",
  "device-7c0eec11-b77f-4f1c-9151-b55f953a0086",
  "device-2b883247-8af4-4f92-b7f7-45c9105e1ae9",
  "device-a28495ac-30c0-4d9a-8cf8-50f36fbb7c9f",
  "device-63dce345-0b41-48bc-bdc4-be3c732c4c0b",
  "device-0131b65c-7788-41bd-a77f-2a6a891dd4d4",
  "device-2a46a394-a22d-4516-906e-2608a21b9c98",
  "device-e1a38eb2-79fa-406a-a63e-bcfa058f990a",
  "device-2682ec3f-a172-45cc-af15-9cab6897574e",
  "device-865d2ce7-0b4a-4b2b-a646-9b1c4cdf8059",
  "device-54a4526b-d68d-4f52-b517-86451ecd794d",
  "device-dd00e630-97ae-42e6-ae9a-1b19daf5e7aa",
  "device-2c94df50-bfd7-483b-891d-0d18cd8da490",
  "device-6c01eb3e-ff84-4e8b-b0a7-91d99c2a9cdc",
  "device-dd590e5e-4407-4278-a823-be54c6f6acc7",
  "device-7cd18dbf-5de8-480c-b167-2128962a070b",
  "device-2bcac2cd-6489-4204-9d51-09fd591af36e",
  "device-6de950cd-5eb8-4b64-b099-0c6db56475a7",
  "device-d329e451-24b8-40d7-9941-1c20f35d5d24",
  "device-f3ccd5fd-b670-4178-b82d-1c66bae445da",
  "device-c7083054-28c2-4fdc-a61a-405cb47052d6",
  "device-7d181eff-0bea-4556-90cb-a22bf000d15d",
  "device-55e08631-1fbe-4eef-887f-1f4397185c78",
  "device-e9c75bb4-f799-472d-a85d-9394fcc5a76f",
  "device-e279d94b-26f2-410f-88a6-7e21394d1155",
  "device-a227567e-be43-471c-b525-1652f0a0d854",
  "device-07610609-1cc6-4da1-beea-5d254cf98352",
  "device-c33da793-a810-4241-b8ec-08ba3814a87d",
  "device-82a9e07b-2909-4307-8e58-916fd63a1e7a",
  "device-76357925-dfd4-412c-8ca6-be967d2c1518",
  "device-9b316410-ee3e-4cef-96aa-9886ea4b0d5b",
  "device-44ec961f-3375-4cd9-8f05-c06ea5fe85e0",
  "device-8a3ced11-f803-42ed-a105-5306c8d2ce71",
  "device-be496f9b-2343-4c8e-ada5-b16b613c7aef",
  "device-dfab2147-ad1e-4868-9d14-dbfc20fd794c",
  "device-341f8497-d8b2-41e0-a2f5-c322c3b895a2",
  "device-8d5139d0-1ca6-4374-b0be-9b5d4829ad0c",
  "device-c1484d24-6fc0-45fa-adcd-41d0323bc9a3",
  "device-33d4e4c5-87c6-4c71-9727-a2e86d2d1efb",
  "device-d09cff92-c989-439d-a0e1-81dd6d306ef9",
  "device-b879ecba-93e4-4f55-bbd3-34cc3bc90e00",
  "device-bf672c83-e929-4f3c-812c-8ff40fe066c6",
  "device-b38112a0-47f2-4fda-9717-3cbecc0bbf05",
  "device-af4e5554-f29c-415c-85cf-4f7f7ce6a06a",
  "device-4c84f48e-17b5-4607-be52-1745cd3c21f6",
  "device-1db96154-6c72-4e38-b737-c2c1c8c44341",
  "device-d6e5ef96-029d-4fa6-a0b3-199c8b598ba2",
  "device-87d4b398-a79f-4f6f-913e-aabed08367b4",
  "device-447c83c0-116d-4c4a-a474-6edde0d8616b",
  "device-73d20012-6845-410f-b937-fe0188209656",
  "device-554e0026-112e-4b8e-bb18-bc9dbbb90b79",
  "device-0c40fd29-de68-4822-bd9f-8608ada97d4c",
  "device-85a34d78-0251-40dd-a390-73bf9e0278ba",
  "device-a0470415-a2b7-4ba3-b4e2-ca1bf287cd29",
  "device-9bf0c3dc-ea36-44d4-9910-e0e2729391e0",
  "device-5806d592-624f-45eb-b7a9-5225dbd0b183",
  "device-04707c90-d817-4276-bef5-ae839481ab98",
  "device-25505a66-e657-4649-bdf8-4c15951bde94",
  "device-a1f21831-9915-4ebd-8854-322873ed53a3",
  "device-a144f37e-31b6-4446-90d4-e05fbc1f3899",
  "device-fde5eaaa-af67-4312-b1cf-48083e797565",
  "device-3d8b0d45-d27b-4d4f-84b0-b53dbacef5df",
  "device-dbd54b98-5f7d-46d5-8f52-a666dbffc4cf",
  "device-bb042f6a-fb4a-4051-89f1-7e87c30f6e98",
  "device-a7f80b23-3699-4c1d-b0ed-c882bc3c3bf7"
)

datapasta::vector_paste()

follow_applicant(df_mp, v_ids[2]) |>
  gt::gtsave("../../../Downloads/hannaford_ok.png")
follow_applicant(df_mp, v_ids[1]) |>
  gt::gtsave("../../../Downloads/hannaford_bad.png")


### understand what happens post search

df_lead <- df_mp |>
  filter(
    !event %in%
      c(
        "ApplicantViewedHelpText",
        "ApplicantOpenedHelpModal",
        "ApplicantViewedHelpTopic"
      )
  ) |>
  clean_events() |>
  mutate(
    event = event |>
      as.factor() |>
      fct_relevel(
        "ApplicantSelectedEmployerOrPlatformItem",
        "ApplicantBeganLinkingEmployer"
      )
  ) |>
  arrange(distinct_id, timestamp, event) |>
  group_by(cbv_flow_id) |>
  mutate(lead_event = lead(event_clean)) |>
  ungroup() |>
  filter(event == "ApplicantSearchedForEmployer")

df_lead |>
  filter(distinct_id %in% c(v_ids)) |>
  mutate(
    lead_event = as.character(lead_event),
    lead_event = ifelse(is.na(lead_event), "Ended Session", lead_event)
  ) |>
  count(lead_event, sort = TRUE) |>
  mutate(
    share = n / sum(n),
    cumsum = cumsum(n) / sum(n)
  ) |>
  print(n = Inf)

v_id <- df_lead |>
  filter(lead_event == "ApplicantAccessedSearchPage") |>
  slice_sample(n = 20) |>
  pull(distinct_id)

follow_applicant(df_mp, v_id[2])

# viz
df_lead |>
  mutate(
    lead_event = as.character(lead_event),
    lead_event = ifelse(is.na(lead_event), "Ended Session", lead_event),
    lead_event = fct_lump(lead_event, prop = .01),
  ) |>
  count(lead_event, sort = TRUE) |>
  mutate(
    share = n / sum(n),
    cumsum = cumsum(n) / sum(n),
    fill_color = ifelse(
      lead_event == "Selected Employer",
      dsac_light_navy,
      dsac_light_cranberry
    ),
    lead_event_n = str_glue("{lead_event} ({n})") |> fct_inorder() |> fct_rev(),
  ) |>
  ggplot(aes(share, lead_event_n, fill = fill_color)) +
  geom_blank(aes(share * 1.05)) +
  geom_col() +
  geom_text(
    aes(label = label_percent(1)(share)),
    family = "Source San 3",
    color = matterhorn,
    hjust = -.2
  ) +
  scale_x_continuous(expand = c(.005, .005)) +
  scale_fill_identity() +
  coord_cartesian(clip = "off") +
  labs(
    x = NULL,
    y = NULL,
    title = "ONLY HALF OF SEARCHES RESULT IN THE USER SELECTING AN EMPLOYER",
    subtitle = "Share of events occuring right after search",
    caption = str_glue(
      "Source: {state} 2026 EMMY Pilot Mixpanel Data [{period}]"
    )
  ) +
  si_style_xgrid() +
  theme(
    legend.position = "none",
    axis.text.x = element_blank()
  )

si_preview()
si_save("Images/search.png")

df_lead |>
  filter()
glimpse()

#clean query

df_lead |>
  filter(lead_event == "Selected Employer") |>
  mutate(
    query_clean = query |>
      str_to_lower() |> # already done
      str_trim() |> # remove leading/trailing whitespace
      str_squish() |> # collapse internal whitespace
      str_remove_all("[^a-z0-9 ]") |> # remove punctuation/special chars
      str_replace_all("\\b(inc|llc|ltd|co|corp|the)\\b", "") |> # strip common suffixes
      str_squish() # squish again after removals
  ) |>
  count(query_clean, sort = TRUE)


df_query <- df_lead |>
  filter(lead_event != "Selected Employer") |>
  mutate(
    query_clean = query |>
      str_to_lower() |> # already done
      str_trim() |> # remove leading/trailing whitespace
      str_squish() |> # collapse internal whitespace
      str_remove_all("[^a-z0-9 ]") |> # remove punctuation/special chars
      str_replace_all("\\b(inc|llc|ltd|co|corp|the)\\b", "") |> # strip common suffixes
      str_squish() # squish again after removals
  )


df_query |>
  count(query_clean, sort = TRUE) |>
  print(n = 50)


# Export masked data -----------------------------------------------------

# Create lookup tables with random masked IDs
user_lookup <- tibble(
  distinct_id = unique(df_query$distinct_id)
) %>%
  mutate(distinct_id_masked = paste0("usr-", sample(sprintf("%04d", 1:n()))))

session_lookup <- data.frame(
  cbv_flow_id = unique(df_query$cbv_flow_id)
) %>%
  mutate(cbv_flow_id_masked = paste0("ses-", sample(sprintf("%04d", 1:n()))))

# Join masked IDs back to your dataframe
df_masked <- df_query %>%
  left_join(user_lookup, by = "distinct_id") %>%
  left_join(session_lookup, by = "cbv_flow_id") %>%
  select(-distinct_id, -cbv_flow_id)

df_masked |>
  mutate(
    missing_results = case_when(
      lead_event == "Accessed Missing Results Page" ~ TRUE
    )
  ) |>
  select(
    user = distinct_id_masked,
    session = cbv_flow_id_masked,
    query_clean,
    missing_results
  ) |>
  write_csv(
    str_glue("Dataout/unmatched_queries_{str_remove_all(today(), '-')}.csv"),
    na = ""
  )


df_query |>
  count(query_clean, sort = TRUE) |>
  slice_head(n = 30) |>
  write_csv("Dataout/top30_unmatched_queries.csv")

#viz
df_query %>%
  count(query_clean, sort = TRUE) %>%
  slice_head(n = 30) %>%
  mutate(
    fill_color = ifelse(
      str_detect(query_clean, "rouse"),
      dsac_light_teal,
      "#909090"
    )
  ) |>
  ggplot(aes(n, fct_reorder(query_clean, n), fill = fill_color)) +
  geom_col() +
  scale_x_continuous(expand = c(.005, .005)) +
  scale_fill_identity() +
  labs(
    x = NULL,
    y = NULL,
    subtitle = "Number of queries for top searched items",
    caption = "Source: Louisiana 2026 EMMY Pilot Mixpanel Data [accessed 2026-03-09]",
    title = "Top Unmatched Search Queries" |> toupper()
  ) +
  si_style_xgrid()

si_preview()
si_save("Images/top_queries_mcdonalds.png")

df_query |>
  unnest_tokens(word, query_clean) |>
  count(word, sort = TRUE) |>
  anti_join(stop_words)


# Compute distance matrix for a sample
sample_queries <- unique(df_query$query_clean)
# sample_queries <- unique(df_query$query_clean)[1:500]  # start small
dist_matrix <- stringdistmatrix(sample_queries, method = "jw") # Jaro-Winkler

# Hierarchical clustering on distance matrix
hclust_result <- hclust(as.dist(dist_matrix), method = "complete")

# Cut tree to form clusters (tune h threshold)
clusters <- cutree(hclust_result, h = 0.2)

clustered_df <- data.frame(
  query_clean = unique(df_query$query_clean),
  cluster_id = clusters
)

# Review clusters
clustered_df %>%
  group_by(cluster_id) %>%
  summarise(
    queries = paste(query_clean, collapse = " | "),
    n = n()
  ) %>%
  filter(n > 1) %>%
  arrange(desc(n)) |>
  View()


df_query <- df_query %>%
  mutate(
    query_keycollision = key_collision_merge(query_clean),
    query_ngram = n_gram_merge(query_clean)
  )

df_query %>%
  filter(query_clean != query_keycollision) %>%
  count(query_clean, query_keycollision, sort = TRUE)


df_query %>%
  filter(str_detect(query_clean, "rouses")) |>
  count(query_clean, sort = TRUE) %>%
  head(30)

### ----------------
v_ids <- df_query |>
  filter(lead_event == "ApplicantSelectedEmployerOrPlatformItem") |>
  slice_sample(n = 10) |>
  pull(distinct_id)

follow_applicant(df_mp, v_ids[5])

key_events
clipr::write_clip(v_ids[2])

df_mp |>
  filter(distinct_id == v_ids[], event == "ApplicantUpdatedSearchTerm") |>
  distinct(query)
df_mp |>
  filter(
    distinct_id == v_ids[3],
    event %in% c("ApplicantSearchedForEmployer", "ApplicantUpdatedSearchTerm")
  ) |>
  extract_properties(term = properties$term) |>
  distinct(query, term)
slice_tail(n = 1) |>
  pull(properties) |>
  pull(term)
glimpse()
View()

df_mp |>
  filter(distinct_id == v_ids[3]) |>
  select(cbv_flow_id, event, timestamp) |>
  View()
