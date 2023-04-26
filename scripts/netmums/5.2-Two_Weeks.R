library(tidyverse)


posts_2_weeks_before = read.csv("~/Documents/dissertation/forum/clean_data/netmums/posts_2_weeks_before.csv")
sentiment_2_weeks_before = posts_2_weeks_before %>%
  select(user_url, min_date) %>%
  mutate(min_date = as.Date(min_date)) %>%
  distinct() %>%
  left_join(df %>% select(user_url, time_since_first_period, compound, day), by="user_url") %>%
  filter(!is.na(day)) %>%
  mutate(day_diff = as.numeric(min_date - day)) %>%
  arrange(user_url, day_diff) %>%
  filter(day_diff > 0 & day_diff <= 14) %>%
  group_by(user_url) %>%
  mutate(period = time_since_first_period - min(time_since_first_period))
  
slope_2_weeks_before = sentiment_2_weeks_before %>%
  select(user_url) %>%
  distinct()

slope_2_weeks_before$slope = 0
slope_2_weeks_before$p = 0

for(i in 1:nrow(slope_2_weeks_before)) {
  user = slope_2_weeks_before$user_url[i]
  sample_df = sentiment_2_weeks_before %>% filter(user_url==user)
  if(nrow(sample_df) > 1) {
    mod = lm(compound ~ period, data=sample_df)
    sample_coefs = summary(mod)$coefficients
    slope_2_weeks_before$slope[i] = sample_coefs[2, 1]
    slope_2_weeks_before$p[i] = sample_coefs[2, 4]
  }
}

slope_2_weeks_before_negative = slope_2_weeks_before %>%
  arrange(slope)

write_csv(slope_2_weeks_before_negative, "~/Documents/dissertation/forum/clean_data/netmums/slope_2_weeks_before_negative.csv")
