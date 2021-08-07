# 2.1.1-Sentiment-Panel_Model.R

# Libraries ----

library(tidyverse)
library(lubridate)
library(plm)
library(zoo)

# read data ----

path_root <- dirname(dirname(dirname(rstudioapi::getSourceEditorContext()$path)))
path_sn <- paste0(path_root, "/clean_data/netmums/sn_sentiment.csv")
path_nt <- paste0(path_root, "/clean_data/netmums/nt_sentiment.csv")
sn_sen <- read.csv(path_sn)
nt_sen <- read.csv(path_nt)

# edit data ----

sn_sen$group <- "sn"
nt_sen$group <- "nt"
sen <- rbind(sn_sen, nt_sen)

sn_sen$yrmo <- substr(sn_sen$date_created, 1, 7)
# sn_sen$date_created <- ymd_hms(sn_sen$date_created)
# sn_sen$yrmo <- as.yearmon(sn_sen$date_created)
sn_months <- sn_sen %>%
  group_by(yrmo, user_url) %>%
  summarize(ave_sen = mean(com_sentiment))

# models ----

ols <- lm(com_sentiment ~ time_since_first_post + user_url, data=sn_sen)
summary(ols)


fe <- plm(com_sentiment ~ time_since_first_post,
          data=sn_sen,
          index=c("user_url"))
summary(fe)
