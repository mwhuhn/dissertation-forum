# 2.1.1-Sentiment-Panel_Model.R

# Libraries ----

library(tidyverse)
library(lubridate)
library(plm)
library(zoo)

# read data ----

path_root <- dirname(dirname(dirname(rstudioapi::getSourceEditorContext()$path)))
# path_sn <- paste0(path_root, "/clean_data/netmums/sn_sentiment.csv")
# path_nt <- paste0(path_root, "/clean_data/netmums/nt_sentiment.csv")
# sn_sen <- read.csv(path_sn)
# nt_sen <- read.csv(path_nt)
path_group <- paste0(path_root, "/clean_data/netmums/group-fe.csv")
group <- read.csv(path_group, stringsAsFactors=FALSE, fileEncoding="UTF-8")

group <- foreign::read.dta("~/Documents/dissertation/forum/clean_data/netmums/daily_propensity_scores.dta")
write.csv(group, "~/Documents/dissertation/forum/clean_data/netmums/daily_propensity_scores.csv", row.names = FALSE)

fe <- plm(pos ~ time_since_first_period*sn_user,
          data=group,
          index=c("user_url", "time_since_first_period"),
          model="within",
          effect="twoways")
summary(fe)



# fix on period and person, not age
# options are period, cohort, and age (two imply three)
