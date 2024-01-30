library(tidyverse)
library(lubridate)
library(MatchIt)

# load data ----
# df = read.csv("~/Documents/dissertation/forum/clean_data/netmums/daily_all.csv", stringsAsFactors = F)
# df$day = as.Date(df$day)
# saveRDS(df, "~/Documents/dissertation/forum/clean_data/netmums/daily_all.rds")
df = readRDS("~/Documents/dissertation/forum/clean_data/netmums/daily_all.rds")

# Create Basic Output Dataset ----
first_day = df %>%
  filter(sn_user==1) %>%
  group_by(user_url) %>%
  summarize(
    day = min(day)
  )
first_sn_day_df <- read.csv("~/Documents/dissertation/forum/clean_data/netmums/first_sn_day.csv", stringsAsFactors = F)

first_sn_day_df = first_sn_day_df %>%
  inner_join(first_day %>% rename(first_day = day), by='user_url') %>%
  mutate(
    first_sn_day = as.Date(first_sn_day),
    total_days_to_first_sn = as.numeric(first_sn_day - first_day)
  )

### Note here: there are fewer SN users in df than in first_sn_day.csv because
### df only contains users with at least 2 observations. Those dropped are
### singletons.


output <- df %>%
  mutate(
    month = month(day),
    year = year(day)
  ) %>%
  select(user_url, day, user_id, month, year, time_since_first_period, sn_user, time_period, neg, neu, pos, compound) %>%
  arrange(time_since_first_period) %>%
  group_by(user_id) %>%
  left_join(first_sn_day_df, by="user_url") %>%
  mutate(
    after_first_sn = 0,
    after_first_sn = if_else(is.na(first_sn_day), 0, if_else(day >= first_sn_day, 1, 0)),
    days_to_first_sn = as.numeric(first_sn_day - day),
    day = as.character(day)
  ) %>%
  select(-first_sn_day, -first_day)

foreign::write.dta(output, "~/Documents/dissertation/forum/clean_data/netmums/daily_propensity_scores_v2.dta")
saveRDS(output, "~/Documents/dissertation/forum/clean_data/netmums/daily_propensity_scores_v2.rds")
output = readRDS("~/Documents/dissertation/forum/clean_data/netmums/daily_propensity_scores_v2.rds")

# Data for matching ----

emotions = names(df)[c(7:10)]
topics = names(df)[c(270:289)]

dominant_emotion = df %>%
  group_by(user_id) %>%
  summarize(
    across(anger:sadness, ~mean(., na.rm=T))
  ) %>%
  rowwise() %>%
  mutate(
    dominant_emotion = emotions[which.max(c_across(anger:sadness))],
  )

dominant_topics = df %>%
  group_by(user_id) %>%
  summarize(
    across(topic_0:topic_19, ~mean(., na.rm=T))
  ) %>%
  rowwise() %>%
  mutate(
    dominant_topics = topics[which.max(c_across(topic_0:topic_19))]
  )

case_matches_in = output %>%
  group_by(user_id) %>%
  summarize(
    n = n(),
    ave_sen = mean(compound),
    first_day = min(time_period),
    post_rate = max(time_since_first_period) / n,
    max_time = max(time_since_first_period),
    sn_user = ifelse(max(sn_user)==1, 1, 0)
  ) %>%
  left_join(dominant_emotion, by="user_id") %>%
  left_join(dominant_topics, by="user_id")

case_matches_in$user_row = rownames(case_matches_in)

saveRDS(case_matches_in, "~/Documents/dissertation/forum/clean_data/netmums/matches_in.rds")
case_matches_in = readRDS("~/Documents/dissertation/forum/clean_data/netmums/matches_in.rds")

# Check initial balance ----

initial.match <- matchit(
  sn_user ~ ave_sen +
    post_rate + max_time + n +
    dominant_emotion + dominant_topics,
  data = case_matches_in,
  method = NULL,
 distance = "glm",
 replace = FALSE
)
summary(initial.match)

# Create matching
genetic.match = matchit(
  sn_user ~ ave_sen +
    post_rate + max_time + n +
    dominant_emotion + dominant_topics,
  data = case_matches_in,
  method = "genetic",
  replace = FALSE
)
saveRDS(genetic.match, "~/Documents/dissertation/forum/clean_data/netmums/genetic_match.rds")

mahalanobis.match = matchit(
  sn_user ~ ave_sen +
    post_rate + max_time + n +
    dominant_emotion + dominant_topics,
  data = case_matches_in,
  distance = "mahalanobis",
  replace = FALSE
)
saveRDS(mahalanobis.match, "~/Documents/dissertation/forum/clean_data/netmums/mahalanobis_match.rds")

glm.match = matchit(
  sn_user ~ ave_sen +
    post_rate + max_time + n +
    dominant_emotion + dominant_topics,
  data = case_matches_in,
  distance = "glm",
  replace = FALSE
)
saveRDS(glm.match, "~/Documents/dissertation/forum/clean_data/netmums/glm_match.rds")

summary(genetic.match)$sum.matched[,1:2]
summary(mahalanobis.match)$sum.matched[,1:2]
summary(glm.match)$sum.matched[,1:2]

### Use genetic match because it has the smallest distances overall

# Extract matches ----


truncation_day = first_sn_day_df %>%
  inner_join(output %>% select(user_url, user_id) %>% unique(), by="user_url") %>%
  select(user_id, total_days_to_first_sn)

match_matrix = as.data.frame(genetic.match$match.matrix)
match_matrix$sn_user_row = rownames(match_matrix)
names(match_matrix)[1] = "nt_user_row"
rownames(match_matrix) = 1:nrow(match_matrix)

match_matrix = match_matrix %>%
  left_join(case_matches_in %>% select(user_id, user_row) %>% rename(sn_user_id = user_id), by=c("sn_user_row"="user_row")) %>%
  left_join(case_matches_in %>% select(user_id, user_row), by=c("nt_user_row"="user_row")) %>%
  left_join(truncation_day, by=c("sn_user_id"="user_id")) %>%
  rename(truncation_day = total_days_to_first_sn) %>%
  select(sn_user_id, user_id, truncation_day)

foreign::write.dta(match_matrix, "~/Documents/dissertation/forum/clean_data/netmums/truncation_matches_v2.dta")
saveRDS(match_matrix, "~/Documents/dissertation/forum/clean_data/netmums/truncation_matches_v2.rds")



# additional controls ----
additional_controls = df %>%
  rowwise() %>%
  mutate(
    dominant_emotion = emotions[which.max(c_across(anger:sadness))],
    dominant_topic = topics[which.max(c_across(topic_0:topic_19))]
  ) %>%
  ungroup() %>%
  mutate(
    across(Activities.and.homeschooling.ideas.for.3.5.year.olds..Key.stage.0.:Young.parents, ~ map_dbl(.x, ~ min(1, .)))
  ) %>%
  mutate(
    n_subforums = rowSums(across(Activities.and.homeschooling.ideas.for.3.5.year.olds..Key.stage.0.:Young.parents))
  ) %>%
  select(user_id, time_since_first_period, dominant_emotion, dominant_topic, n_subforums, daily_count, days_since_last_post)
foreign::write.dta(additional_controls, "~/Documents/dissertation/forum/clean_data/netmums/additional_controls_v2.dta")
saveRDS(additional_controls, "~/Documents/dissertation/forum/clean_data/netmums/additional_controls_v2.rds")
