library(glmnet)
library(survival)
library(tidyverse)
library(lubridate)

# load data
df <- read.csv("~/Documents/dissertation/forum/clean_data/netmums/daily_all.csv", stringsAsFactors = F)
df$day <- as.Date(df$day)
df <- df[df$day < as.Date("2021-01-01"), ]
df$time_period <- df$time_period + 1

# cox columns
# data is filtered from start of forum to end of 2020
# time: time_period
# exit: is_last_period
# cluster(id): user_id

df <- df %>%
  mutate(
    across(Activities.and.homeschooling.ideas.for.3.5.year.olds..Key.stage.0.:Young.parents, ~ map_dbl(.x, ~ min(1, .)))
  )

df <- df %>%
  mutate(
    n_subforums = rowSums(across(Activities.and.homeschooling.ideas.for.3.5.year.olds..Key.stage.0.:Young.parents))
  )

# columns
all_cols <- names(df)
all_cols
X_cols <- all_cols[c(7:10, 11:289, 297)]
# x_cols.counts <- all_cols[c(3:6, 11:289, 297)]

# lasso model
X <- as.matrix(df[, X_cols])
# res.lasso <- glmnet(X, Surv(df$time_period, df$is_last_period), family="cox")
cv.lasso <- cv.glmnet(X, Surv(df$time_period, df$is_last_period), family="cox", type.measure="C")
plot(cv.lasso)
# 
coef.1se <- coef(cv.lasso, s=cv.lasso$lambda.1se)
coef.min <- coef(cv.lasso, s=cv.lasso$lambda.min)

get_cols <- function(x) {
  return(x@Dimnames[[1]][x@i+1])
}

cols.1se <- get_cols(coef.1se)
cols.min <- get_cols(coef.min)

saveRDS(cv.lasso, "~/Documents/dissertation/forum/clean_data/netmums/cv_lasso_cox.rds")

save.image("~/Documents/dissertation/forum/clean_data/netmums/propensity_wksp.RData")

load("~/Documents/dissertation/forum/clean_data/netmums/propensity_wksp.RData")

rm(X)
rm(cv.lasso)
# drop unused columns

  
# cox model

cox_formula <- "Surv(time_period, is_last_period) ~ "
for(i in 1:length(cols.1se)) {
  if(i == 1) {
    cox_formula <- paste0(cox_formula, cols.1se[i])
  } else {
    cox_formula <- paste0(cox_formula, " + ", cols.1se[i])
  }
}
cox_formula <- paste0(cox_formula, " + cluster(user_id)")

res.cox <- coxph(as.formula(cox_formula), data=df)

saveRDS(res.cox, "~/Documents/dissertation/forum/clean_data/netmums/cox_model.rds")

df <- df %>%
  mutate(
    i = row_number()
  )

# Iterate through chunks of the hazard probability
# data and pull just the probabilities
starts <- seq(1, nrow(df), by=10000)
ends <- c(starts[2:length(starts)] - 1, nrow(df))
for(n in 1:length(starts)) {
  start <- starts[n]
  end <- ends[n]
  print(paste0("start: ", start))
  chunk <- df[start:end, ]
  probs.cox <- survfit(res.cox, newdata=chunk)$surv
  chunk <- chunk %>%
    mutate(
      row_n = row_number(),
      p = map2(row_n, time_period, ~ probs.cox[.y, .x][[1]])
    ) %>%
    select(i, p)
  if(n == 1) {
    out <- chunk
  } else {
    out <- rbind(out, chunk)
  }
}

out$p <- unlist(out$p)

saveRDS(out, "~/Documents/dissertation/forum/clean_data/netmums/propensity_scores.rds")
saveRDS(cols.1se, "~/Documents/dissertation/forum/clean_data/netmums/propensity_score_cols.rds")

first_day = df %>%
  filter(sn_user==1) %>%
  group_by(user_url) %>%
  summarize(
    day = min(day)
  )
first_sn_day_df <- read.csv("~/Documents/dissertation/forum/clean_data/netmums/first_sn_day.csv", stringsAsFactors = F)
first_sn_day_df = first_sn_day_df %>%
  mutate(
    first_sn_day = as.Date(first_sn_day)
  ) %>%
  left_join(first_day, by="user_url") %>%
  mutate(
    first_sn_day = if_else(day > first_sn_day, day, first_sn_day)
  ) %>%
  select(-day)

output <- df %>%
  mutate(
    month = month(day),
    year = year(day)
  ) %>%
  select(user_url, day, user_id, month, year, time_since_first_period, sn_user, time_period, neg, neu, pos, compound) %>%
  bind_cols(out) %>%
  arrange(time_since_first_period) %>%
  group_by(user_id) %>%
  left_join(first_sn_day_df, by="user_url") %>%
  mutate(
    after_first_sn = 0,
    after_first_sn = if_else(is.na(first_sn_day), 0, if_else(day >= first_sn_day, 1, 0)),
    days_to_first_sn = as.numeric(first_sn_day - day),
    first_p = first(p),
    inv_p = 1 / first_p,
    day = as.character(day)
  ) %>%
  select(-first_sn_day)


foreign::write.dta(output, "~/Documents/dissertation/forum/clean_data/netmums/daily_propensity_scores.dta")


save.image("~/Documents/dissertation/forum/clean_data/netmums/propensity_wksp.RData")

load("~/Documents/dissertation/forum/clean_data/netmums/propensity_wksp.RData")

potentials <- df %>%
  filter(sn_user == 1) %>%
  group_by(user_url) %>%
  summarise(
    time_on_forum = max(time_since_first_period),
    days = n()
  ) %>%
  filter(days >= 2)

potentials$b <- 0
potentials$p <- 0

for(i in 1:nrow(potentials)) {
  mod <- summary(lm(pos ~ time_since_first_period, data=df[df$user_url==potentials[i, "user_url"][[1]],]))
  if(!is.nan(mod$coefficients[2])) {
    potentials$b[i] <- mod$coefficients[2, 1]
    potentials$p[i] <- mod$coefficients[2, 4]
  }
}

save.image("~/Documents/dissertation/forum/clean_data/netmums/potentials.RData")

most <- read.csv("~/Documents/dissertation/forum/clean_data/netmums/most_sn_posts.csv", stringsAsFactors = F)
most <- merge(potentials, most, by = "user_url", all.x=TRUE)


# save for stata xtheckman


df %>%
  select(day, user_id, time_since_first_period, sn_user, time_period, neg, neu, pos, compound, all_of(cols.1se)) %>%
  mutate(
    month = month(day),
    year = year(day)
  ) %>%
  foreign::write.dta("~/Documents/dissertation/forum/clean_data/netmums/fe_xtheckman.dta")

df %>%
  group_by(user_id) %>%
  summarise(
    is_same = min(sn_user) == max(sn_user)
  ) %>%
  select(is_same) %>%
  table()




summary <- output %>%
  group_by(user_id) %>%
  summarize(
    min = min(time_since_first_period),
    max = max(time_since_first_period),
    n = n(),
    sn_user = max(sn_user),
    pos_sen = mean(pos)
  ) %>%
  group_by(sn_user) %>%
  summarize(
    ave_sen = mean(pos_sen),
    ave_time = mean(max),
    ave_n = mean(n),
    rate = ave_n / ave_time
  )

t.test(output$pos ~ output$sn_user)

counts <- output %>%
  select(user_id, sn_user) %>%
  distinct()
table(counts$sn_user)

month_counts <- output %>%
  group_by(yymm=floor_date(ymd(day), "month"), sn_user) %>%
  summarize(
    n = n()
  )
ggplot(data=month_counts, aes(x=yymm, y=n, color=as.factor(sn_user))) +
  geom_line()



# matching ----

# truncation = output %>%
#   arrange(time_since_first_period) %>%
#   group_by(user_id) %>%
#   filter(row_number() == n())
# 
# ggplot(data=truncation, aes(time_since_first_period, fill=factor(sn_user))) +
#   geom_histogram(bins=100) +
#   scale_x_continuous(limits = c(0, 365))


truncation_day = output %>%
  filter(sn_user==1 & after_first_sn == 1) %>%
  arrange(time_since_first_period) %>%
  group_by(user_id) %>%
  filter(row_number()==1) %>%
  select(user_id, time_since_first_period)

# # todo: use df instead of output for this
# case_matches_in = output %>%
#   group_by(user_id) %>%
#   summarize(
#     n = n(),
#     min_sen = min(compound),
#     max_sen = max(compound),
#     ave_sen = mean(compound),
#     first_day = min(time_period),
#     post_days = max(time_since_first_period) / n,
#     not_sn_user = ifelse(max(sn_user)==1, 0, 1)
#   )
# case_matches_in$user_row = rownames(case_matches_in)
# 
# 
# m.out <- matchit(not_sn_user ~ min_sen + max_sen + ave_sen +
#                    first_day + post_days + n, 
#                  data = case_matches_in, method="optimal",
#                  replace = TRUE)
# summary(m.out)
# match_matrix = as.data.frame(m.out$match.matrix)
# match_matrix$not_sn_user_row = rownames(match_matrix)
# names(match_matrix)[1] = "sn_user_row"
# rownames(match_matrix) = 1:nrow(match_matrix)
# match_matrix = match_matrix %>%
#   left_join(case_matches_in %>% select(user_id, user_row) %>% rename(sn_user_id = user_id), by=c("sn_user_row"="user_row")) %>%
#   left_join(case_matches_in %>% select(user_id, user_row), by=c("not_sn_user_row"="user_row")) %>%
#   left_join(truncation_day, by=c("sn_user_id"="user_id")) %>%
#   rename(truncation_day = time_since_first_period) %>%
#   select(user_id, truncation_day)
# 
# foreign::write.dta(match_matrix, "~/Documents/dissertation/forum/clean_data/netmums/truncation_matches.dta")
# 
# 
# 
# 
# 
# # todo: use df instead of output for this
# case_matches_in = output %>%
#   group_by(user_id) %>%
#   summarize(
#     n = n(),
#     min_sen = min(compound),
#     max_sen = max(compound),
#     ave_sen = mean(compound),
#     first_day = min(time_period),
#     post_days = max(time_since_first_period) / n,
#     sn_user = ifelse(max(sn_user)==1, 1, 0)
#   )
# case_matches_in$user_row = rownames(case_matches_in)
# 
# 
# m.out <- matchit(sn_user ~ min_sen + max_sen + ave_sen +
#                    first_day + post_days + n, 
#                  data = case_matches_in, method="nearest",
#                  replace = FALSE)
# summary(m.out)
# match_matrix = as.data.frame(m.out$match.matrix)
# match_matrix$sn_user_row = rownames(match_matrix)
# names(match_matrix)[1] = "not_sn_user_row"
# rownames(match_matrix) = 1:nrow(match_matrix)
# match_matrix = match_matrix %>%
#   left_join(case_matches_in %>% select(user_id, user_row) %>% rename(sn_user_id = user_id), by=c("sn_user_row"="user_row")) %>%
#   left_join(case_matches_in %>% select(user_id, user_row), by=c("not_sn_user_row"="user_row")) %>%
#   left_join(truncation_day, by=c("sn_user_id"="user_id")) %>%
#   rename(truncation_day = time_since_first_period) %>%
#   select(user_id, truncation_day)
# 
# foreign::write.dta(match_matrix, "~/Documents/dissertation/forum/clean_data/netmums/truncation_matches.dta")
# 
# count_user_ids = match_matrix %>%
#   group_by(V1) %>%
#   summarize(n=n())



# Method Used:

emotions = names(df)[c(7:10)]
topics = names(df)[c(270:289)]

dominant_emotion_df = df %>%
  rowwise() %>%
  mutate(
    dominant_emotion = emotions[which.max(c_across(anger:sadness))],
  ) %>%
  group_by(user_id, dominant_emotion) %>%
  count() %>%
  ungroup(dominant_emotion) %>%
  slice(which.max(n)) %>%
  select(-n)

dominant_topics_df = df %>%
  rowwise() %>%
  mutate(
    dominant_topics = topics[which.max(c_across(topic_0:topic_19))]
  ) %>%
  group_by(user_id, dominant_topics) %>%
  count() %>%
  ungroup(dominant_topics) %>%
  slice(which.max(n)) %>%
  select(-n)

case_matches_in = output %>%
  group_by(user_id) %>%
  summarize(
    n = n(),
    ave_sen = mean(compound),
    first_day = min(time_period),
    post_days = max(time_since_first_period) / n,
    sn_user = ifelse(max(sn_user)==1, 1, 0)
  ) %>%
  left_join(dominant_emotion_df, by="user_id") %>%
  left_join(dominant_topics_df, by="user_id")
case_matches_in$user_row = rownames(case_matches_in)


m.out <- matchit(sn_user ~ ave_sen +
                   first_day + post_days + n +
                   dominant_emotion + dominant_topics, 
                 data = case_matches_in, distance="mahalanobis",
                 replace = FALSE)
summary(m.out)
match_matrix = as.data.frame(m.out$match.matrix)
match_matrix$sn_user_row = rownames(match_matrix)
names(match_matrix)[1] = "not_sn_user_row"
rownames(match_matrix) = 1:nrow(match_matrix)
match_matrix = match_matrix %>%
  left_join(case_matches_in %>% select(user_id, user_row) %>% rename(sn_user_id = user_id), by=c("sn_user_row"="user_row")) %>%
  left_join(case_matches_in %>% select(user_id, user_row), by=c("not_sn_user_row"="user_row")) %>%
  left_join(truncation_day, by=c("sn_user_id"="user_id")) %>%
  rename(truncation_day = time_since_first_period) %>%
  select(sn_user_id, user_id, truncation_day)

foreign::write.dta(match_matrix, "~/Documents/dissertation/forum/clean_data/netmums/truncation_matches.dta")


dominant_emotion_matched = df %>%
  filter(user_id %in% match_matrix$user_id | user_id %in% match_matrix$sn_user_id) %>%
  left_join(match_matrix %>% select(user_id, truncation_day), by="user_id") %>%
  filter(is.na(truncation_day) | truncation_day > time_since_first_period) %>%
  select(-truncation_day) %>%
  left_join(match_matrix  %>% select(sn_user_id, truncation_day), by=c("user_id"="sn_user_id")) %>%
  filter(is.na(truncation_day) | truncation_day > time_since_first_period) %>%
  select(-truncation_day) %>%
  rowwise() %>%
  mutate(
    dominant_emotion = emotions[which.max(c_across(anger:sadness))],
  ) %>%
  group_by(user_id, dominant_emotion) %>%
  count() %>%
  ungroup(dominant_emotion) %>%
  slice(which.max(n)) %>%
  select(-n)

dominant_topics_matched = df %>%
  filter(user_id %in% match_matrix$user_id | user_id %in% match_matrix$sn_user_id) %>%
  left_join(match_matrix %>% select(user_id, truncation_day), by="user_id") %>%
  filter(is.na(truncation_day) | truncation_day > time_since_first_period) %>%
  select(-truncation_day) %>%
  left_join(match_matrix  %>% select(sn_user_id, truncation_day), by=c("user_id"="sn_user_id")) %>%
  filter(is.na(truncation_day) | truncation_day > time_since_first_period) %>%
  select(-truncation_day) %>%
  rowwise() %>%
  mutate(
    dominant_topics = topics[which.max(c_across(topic_0:topic_19))]
  ) %>%
  group_by(user_id, dominant_topics) %>%
  count() %>%
  ungroup(dominant_topics) %>%
  slice(which.max(n)) %>%
  select(-n)

matched_users = output %>%
  filter(user_id %in% match_matrix$user_id | user_id %in% match_matrix$sn_user_id) %>%
  left_join(match_matrix %>% select(user_id, truncation_day), by="user_id") %>%
  filter(is.na(truncation_day) | truncation_day > time_since_first_period) %>%
  select(-truncation_day) %>%
  left_join(match_matrix  %>% select(sn_user_id, truncation_day), by=c("user_id"="sn_user_id")) %>%
  filter(is.na(truncation_day) | truncation_day > time_since_first_period) %>%
  select(-truncation_day) %>%
  group_by(user_id) %>%
  summarize(
    n = n(),
    min_sen = min(compound),
    max_sen = max(compound),
    ave_sen = mean(compound),
    first_day = min(time_period),
    post_days = max(time_since_first_period) / n,
    sn_user = ifelse(max(sn_user)==1, 1, 0)
  ) %>%
  left_join(dominant_emotion_matched, by="user_id") %>%
  left_join(dominant_topics_matched, by="user_id")

t.test(matched_users$ave_sen ~ matched_users$sn_user)
t.test(matched_users$first_day ~ matched_users$sn_user)
t.test(matched_users$n ~ matched_users$sn_user)
t.test(matched_users$post_days ~ matched_users$sn_user)
# t.test(matched_users$min_sen ~ matched_users$sn_user)
# t.test(matched_users$max_sen ~ matched_users$sn_user)
chisq.test(matched_users$sn_user, matched_users$dominant_emotion)
table(matched_users$sn_user, matched_users$dominant_emotion)
chisq.test(matched_users$sn_user, matched_users$dominant_topics)
table(matched_users$sn_user, matched_users$dominant_topics)


sen_averages = output %>%
  group_by(user_id, after_first_sn, sn_user) %>%
  summarize(ave_sen = mean(compound))

t.test(sen_averages$ave_sen[sen_averages$sn_user==1] ~ sen_averages$after_first_sn[sen_averages$sn_user==1])
t.test(sen_averages$ave_sen[sen_averages$after_first_sn==0] ~ sen_averages$sn_user[sen_averages$after_first_sn==0])


# additional controls ----
additional_controls = df %>%
  rowwise() %>%
  mutate(
    dominant_emotion = emotions[which.max(c_across(anger:sadness))],
    dominant_topic = topics[which.max(c_across(topic_0:topic_19))]
  ) %>%
  ungroup() %>%
  select(user_id, time_since_first_period, dominant_emotion, dominant_topic, n_subforums, daily_count, days_since_last_post)
foreign::write.dta(additional_controls, "~/Documents/dissertation/forum/clean_data/netmums/additional_controls.dta")
