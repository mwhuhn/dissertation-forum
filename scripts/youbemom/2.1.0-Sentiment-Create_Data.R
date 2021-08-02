# 2.1.0 Sentiment-Create_Dataset_Youbemom

# Libraries ----

library(tidyverse)
library(RSQLite)
library(DBI)

# Path ----

path_parent <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))

# Load files ----

fn <- paste0(path_parent, "/database/youbemom-merged.db")
conn <- dbConnect(SQLite(), fn)

## . SQL for selecting data ----

select_sql <- "
    SELECT
        p.message_id AS message_id,
        p.date_created AS date_created,
        p.parent_id AS parent_id,
        s.neg_sen_clean as neg_sen_clean,
        s.neu_sen_clean as neu_sen_clean,
        s.pos_sen_clean as pos_sen_clean,
        s.com_sen_clean as com_sen_clean
    FROM posts AS p
    JOIN sentiment AS s
    ON p.message_id = s.message_id
    WHERE p.subforum = '%s'
      AND p.message_id IN (
        SELECT message_id
        FROM text
        WHERE text_clean<>''
          AND probable_spam=0
      )
"

## . Special needs ----

sn_sql <- sprintf(select_sql, "special-needs")
sn <- dbGetQuery(conn, sn_sql)

## . Tweens/teens ----

tt_sql <- sprintf(select_sql, "tween-teen")
tt <- dbGetQuery(conn, tt_sql)

## . Elementary ----

el_sql <- sprintf(select_sql, "elementary")
el <- dbGetQuery(conn, el_sql)

## . Preschool ----

pr_sql <- sprintf(select_sql, "preschool")
pr <- dbGetQuery(conn, pr_sql)

## . Newborn ----

nb_sql <- sprintf(select_sql, "newborn")
nb <- dbGetQuery(conn, nb_sql)

## . NYC ----

nyc_sql <- sprintf(select_sql, "new-york-city")
nyc <- dbGetQuery(conn, nyc_sql)

## . Toddler ----

if(dbExistsTable(conn, "temp")) {
  dbRemoveTable(conn, "temp")
}
td_ids_sql <- " SELECT family_id FROM threads WHERE subforum='toddler' "
td_ids <- dbGetQuery(conn, td_ids_sql)
set.seed(291)
per <- 0.1
td_ids_sample <- slice_sample(td_ids, prop = per)
dbCreateTable(conn, "temp", td_ids_sample, temporary = T)
dbWriteTable(conn, "temp", td_ids_sample, overwrite=T)
td_sql <- "
    SELECT
        p.message_id AS message_id,
        p.date_created AS date_created,
        p.parent_id AS parent_id,
        s.neg_sen_clean as neg_sen_clean,
        s.neu_sen_clean as neu_sen_clean,
        s.pos_sen_clean as pos_sen_clean,
        s.com_sen_clean as com_sen_clean
    FROM posts AS p
    JOIN sentiment AS s
    ON p.message_id = s.message_id
    WHERE p.family_id IN (SELECT family_id FROM temp)
"
td <- dbGetQuery(conn, td_sql)

## . Disconnect and clean env ----

dbDisconnect(conn)
rm(list=c("conn", "td_ids", "td_ids_sample", "per"))
rm(list=ls(pattern="_sql"))

## . Nest data ----

sen <- tibble(
  subforum = c("el", "nb", "nyc", "pr", "sn", "td", "tt"),
  data = list(el, nb, nyc, pr, sn, td, tt)
)

## . Write data to file ----
out <- paste0(path_parent, "/clean_data/sentiment.rds")
write_rds(sen, out)

# sen <- sen %>%
#   mutate(model = map(data, function(df) lm(com_sen_clean ~ neg_sen_clean, data = df)))
# sen <- sen %>%
#   mutate(pred = map(model, predict))

# Clean data ----

days <- c("Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday")
months <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")
periods <- c("before","march","during")

pad <- function(n) {
  if(is.character(n)) {
    t <- as.numeric(n)
  } else {
    t <- n
  }
  if(t < 10) {
    return(paste0("0", as.character(t)))
  } else {
    return(as.character(t))
  }
}

weeks <- map_chr(c(1,14,27,40), pad)
wy_scale <- expand.grid(w=weeks, y=2014:2020) %>%
  select(y, w) %>%
  apply(1, paste, collapse="-")

clean_data <- function(df) {
  df <- df %>%
    mutate(
      date_created = ymd_hms(date_created, tz="EST"),
      year_n = year(date_created),
      day_ymd = as_date(date_created),
      week_n = week(date_created),
      week_y = floor_date(date_created, unit = "week"),
      month_n = month(date_created),
      is_parent = is.na(parent_id)
    ) %>%
    unite("month_y", year_n, month_n, sep="-", remove=F) %>%
    mutate(
      month_y = as.yearmon(month_y)
    ) %>%
    filter(
      date_created < ymd(20210101) & date_created >= ymd(20140101)
    )
  return(df)
}

sn <- clean_data(sn)
td <- clean_data(td)

# Plot by year-week ----

## . plot frequency  ----

plot_freq <- function(df) {
  df %>%
    group_by(month_y) %>%
    summarise(
      freq = n(),
      .groups = 'drop_last'
    ) %>%
    ggplot(aes(x=month_y, y=freq)) +
    geom_point() +
    geom_line()
}

plot_freq(sn)
plot_freq(td)


plot_freq_year <- function(df) {
  df %>%
    group_by(month_n, year_n) %>%
    summarise(
      freq = n(),
      .groups = 'drop_last'
    ) %>%
    ggplot(aes(x=month_n, y=freq, group=year_n, color=as.factor(year_n))) +
    geom_point() +
    geom_line() +
    scale_x_continuous(breaks=seq(1,12,1))
}

plot_freq_year(sn)
plot_freq_year(td)

plot_freq_detrend <- function(df) {
  dat <- sn %>%
    group_by(month_n, year_n) %>%
    summarise(
      freq = n(),
      .groups = 'drop_last'
    ) %>%
    unite("month_y", year_n, month_n, sep="-", remove=F) %>%
    mutate(
      month_y = as.yearmon(month_y)
    ) %>%
    arrange(year_n, month_n)
  mod <- lm(freq ~ month_y, data=dat)
  dat$preds <- predict(mod)
  dat$detrend <- dat$freq - dat$preds
  dat %>%
    ggplot(aes(x=month_n, y=detrend, group=year_n, color=as.factor(year_n))) +
    geom_point() +
    geom_line() +
    scale_x_continuous(breaks=1:12)
}

plot_freq_detrend(sn)

## . plot ave sentiment ----

plot_sen <- function(df) {
  df %>%
    group_by(week_y) %>%
    summarise(
      ave_sen = mean(com_sen_clean),
      .groups = 'drop_last'
    ) %>%
    ggplot(aes(x=week_y, y=ave_sen)) +
    geom_point() +
    geom_line() +
    theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5))
}

plot_sen(sn)
plot_sen(td)

plot_sen_smooth <- function(df) {
  df %>%
    group_by(month_y) %>%
    summarise(
      ave_sen = mean(com_sen_clean),
      .groups = 'drop_last'
    ) %>%
    ggplot(aes(x=month_y, y=ave_sen)) +
    geom_smooth() +
    theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5))
}

plot_sen_smooth(sn)
plot_sen_smooth(td)

plot_sen_detrend <- function(df) {
  dat <- sn %>%
    group_by(month_n, year_n) %>%
    summarise(
      ave_sen = mean(com_sen_no_url),
      .groups = 'drop_last'
    ) %>%
    unite("month_y", year_n, month_n, sep="-", remove=F) %>%
    mutate(
      month_y = as.yearmon(month_y)
    ) %>%
    arrange(year_n, month_n)
  mod <- lm(ave_sen ~ month_y, data=dat)
  dat$preds <- predict(mod)
  dat$detrend <- dat$ave_sen - dat$preds
  dat %>%
    ggplot(aes(x=month_n, y=detrend, group=year_n, color=as.factor(year_n))) +
    geom_point() +
    geom_line() +
    scale_x_continuous(breaks=1:12)
}

sn %>%
  filter(
    date_created < ymd(20200501) & date_created >= ymd(20200101)
  ) %>%
  mutate(
    period = case_when(
      date_created < ymd(20200301) ~ "before",
      date_created >= ymd(20200301) ~ "during",
    )
  ) %>%
  group_by(period) %>%
  summarise(
    ave_sen = mean(com_sen_clean),
    .groups = 'drop_last'
  ) %>%
  ggplot(aes(x=period, y=ave_sen)) +
  geom_col()

plot_sen_detrend(sn)

#  Diff in diff

sn.trend <- lm(com_sen_clean ~ day_ymd, data=sn)
summary(sn.trend)
# Sentiment has a negative trend on the special-needs board

td.trend <- lm(com_sen_clean ~ day_ymd, data=td)
summary(td.trend)

sn$forum <- 1
td$forum <- 0
did <- rbind(sn, td)
did <- did %>%
  mutate(
    time = ifelse(date_created < ymd(20200301), 0, 1),
    week_n = week(date_created),
    year_n = year(date_created)
  )

did %>%
  group_by(month_y, forum) %>%
  summarise(
    ave_sen = mean(com_sen_clean),
    .groups = 'drop_last'
  ) %>%
  ggplot(aes(x=month_y, y=ave_sen, group=forum, color=factor(forum))) +
  geom_point() +
  geom_line() +
  stat_smooth(method=lm) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5)) +
  scale_color_discrete("Subforum", labels=c("Toddler", "Special\nNeeds")) +
  labs(title="Sentiment Trend",
       x="",
       y="Average Monthly\nCompound Sentiemnt",
       caption="Sentiment calcualted from text with no urls.") +
  theme_classic() +
  theme(
    text = element_text(family = "montserrat", size=20)
  )
ggsave(paste0(path_parent,"/plots/ave_montly_sen_by_forum.png"),
       width=8,
       height=5,
       units="in")

did1 <- lm(com_sen_clean ~ forum * time + factor(week_n), data = did)
summary(did1)

did.pred1 <- expand.grid(year_n=2014:2020,
                       week_n=1:53,
                       forum=0:1,
                       time=0:1)
did.pred1$sen <- predict(did1, did.pred1)
did.pred1 %>%
  mutate(
    week_n = map_chr(week_n, pad),
    wy = paste(year_n, week_n, sep="-")
  ) %>%
  ggplot(aes(x=wy, y=sen, group=forum,  color=factor(forum))) +
  geom_smooth() +
  scale_x_discrete(breaks=wy_scale, labels=wy_scale) +
  theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))

did.pred2 <- expand.grid(forum=0:1,
                         time=0:1)
did.pred2$year_n <- c(2019, 2019, 2020, 2020)

did.bd <- did %>%
  filter(
    date_created < ymd(20200501) & date_created >= ymd(20200101)
  ) %>%
  mutate(
    period = case_when(
      time == 1 ~ "During",
      time == 0 ~ "Before"
    ),
    forum_n = case_when(
      forum == 1 ~ "Special-Needs",
      forum == 0 ~ "Toddler"
    )
  )
did.bd %>%
  group_by(period, forum_n) %>%
  summarise(
    ave_sen = mean(com_sen_clean),
    .groups = 'drop_last'
  ) %>%
  ggplot(aes(x=forum_n, y=ave_sen, group=period, fill=period)) +
  geom_col(position = "dodge", color="black") +
  labs(
    title="Sentiment Jan - April 2020",
    x="Forum",
    y="Average Compound Sentiment"
  ) +
  theme_classic() +
  theme(
    text = element_text(family = "montserrat", size=20)
  )

ggsave(paste0(path_parent,"/plots/before-during_sentiment.png"),
       width=8,
       height=5,
       units="in")

t.test(did.bd$com_sen_clean[did.bd$forum_n=="Special-Needs"]~did.bd$period[did.bd$forum_n=="Special-Needs"])
t.test(did.bd$com_sen_clean[did.bd$forum_n=="Toddler"]~did.bd$period[did.bd$forum_n=="Toddler"])
# interaction is significant p < 0.01
# being sn makes it more positive
# being during pandemic makes it more negative
# being sn and during pandemic makes it more negative
# diff in diff suggests sn experiences more distress during this time
did2 <- lm(com_sen_no_url ~ forum * time + factor(week_n), data = did)
summary(did2)

forum_labels <- c("Toddler", "Special Needs")
names(forum_labels) <- c("0", "1")
# TODO: normalize data between sn and td?
did %>%
  group_by(week_n, year_n, forum) %>%
  summarise(
    ave_sen = mean(com_sen_clean),
    .groups = 'drop_last'
  ) %>%
  filter(
    year_n < 2021
  ) %>%
  mutate(
    color = ifelse(year_n == 2020, 1, 0)
  ) %>%
  arrange(year_n, week_n) %>%
  ggplot(aes(x=week_n, y=ave_sen, group=as.factor(year_n), color=as.factor(color))) +
  geom_rect(xmin = 9.86, xmax=14.29, ymin=-Inf, ymax=Inf, alpha=.2, fill="grey90", inherit.aes = F) +
  geom_point() +
  geom_line() +
  facet_wrap(~forum, labeller=labeller(forum=forum_labels)) +
  scale_color_manual("Year", values=c("grey60", "firebrick2"), breaks=c(0, 1), labels=c("2014-2019", "2020")) +
  theme_classic() +
  labs(title="Average Weekly Sentiment by Year",
       x="Week",
       y="Average Compound Sentiment") +
  theme(
    text = element_text(family = "montserrat", size=20)
  )
  
ggsave(paste0(path_parent,"/plots/ave_weekly_sen_by_year.png"),
       width=8,
       height=5,
       units="in")

# Time Series ----

dates <- seq(as.Date("2014-01-01"), as.Date("2020-11-30"), by="days")
dates <- data.frame(d=dates[!grepl("-02-29", dates)])

to_ts <- function(df) {
  daily_sen <- df %>%
    mutate(
      d = as_date(date_created)
    ) %>%
    group_by(d) %>%
    summarise(
      ave_sen = mean(com_sen_clean),
      .groups = 'drop_last'
    ) %>%
    arrange(d) %>%
    select(d, ave_sen) %>%
    filter(
      !grepl("-02-29", d)
    ) %>%
    right_join(dates, by="d") %>%
    mutate(
      ave_sen = replace_na(ave_sen, 0)
    )
  return(ts(deframe(daily_sen), start=c(2014, 1), frequency=365))
}

sn_ts <- to_ts(sn)
td_ts <- to_ts(td)

plot(sn_ts)
sn_ts_l12 <- diff(sn_ts, lag = 12)
plot(sn_ts_l12)

adf.test(diff(td_ts))
adf.test(diff(sn_ts))
ndiffs(td_ts, test = "adf")
ndiffs(sn_ts, test = "adf")

kpss.test(td_ts)
kpss.test(sn_ts)
ndiffs(td_ts, test = "kpss")
ndiffs(sn_ts, test = "kpss")

Box.test(td_ts)
Box.test(sn_ts)


pacf(sn_ts)

fit_td <- stl(td_ts, s.window="period")
plot(fit_td)
fit_sn <- stl(sn_ts, s.window="period")
plot(fit_sn)

monthplot(sn_ts)
seasonplot(sn_ts) 

sn_dec <- decompose(sn_ts, type="additive") 
plot(sn_dec)

stl(sn_ts)

