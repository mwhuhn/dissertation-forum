# 2.1.1 Sentiment-Clean_Data_Youbemom

# Libraries ----

library(tidyverse)
library(forecast)
library(lubridate)
library(zoo)
library(tseries)

# Path ----

path_parent <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))

# Load files ----

fn <- paste0(path_parent, "/clean_data/sentiment.rds")
sen <- read_rds(fn)
ox <- read.csv("https://raw.githubusercontent.com/OxCGRT/USA-covid-policy/master/data/OxCGRT_US_latest.csv")
ox <- ox %>%
  filter(Jurisdiction=="NAT_GOV")

# Clean data ----

clean_data <- function(df) {
    df %>% mutate(
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
}

test <- sen %>%
  mutate(data =  map(data, ~
                     .x %>%
                     clean_data))
sen <- sen %>%
  mutate(model = map(data, function(df) lm(com_sen_clean ~ neg_sen_clean, data = df)))

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

plot_freq(test$data[[1]])
