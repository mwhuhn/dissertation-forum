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


fe <- plm(mean_com_sen ~ 1,
          data=group,
          index=c("user_url", "ymd"),
          model="within",
          effect="twoways")
summary(fe)



# fix on period and person, not age
# options are period, cohort, and age (two imply three)
