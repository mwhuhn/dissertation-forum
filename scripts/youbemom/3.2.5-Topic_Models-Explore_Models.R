# 3.2.5 Explore Topic Models

# Libraries ----

library(tidyverse)
library(lubridate)
library(zoo)
library(viridis)

# Path ----

path_parent <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))

# Load files ----

sn <- read.csv(paste0(path_parent,"/clean_data/topic_model_special-needs_parent.txt"), sep="\t")

# Clean Data ----

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
      month_n = month(date_created)
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

# Frequency

filt <- sn %>%
  filter(
    date_created < ymd(20210101) & date_created >= ymd(20190101)
  ) %>%
  mutate(
    week_n = map_chr(week_n, pad),
    wy = paste(year_n, week_n, sep="-")
  ) %>%
  group_by(wy, Dominant_Topic) %>%
  summarise(
    n = n(),
    .groups = 'drop_last'
  ) %>%
  mutate(
    rel_freq = n / sum(n)
  ) %>%
  ggplot(aes(x=wy, y=rel_freq, fill=as.factor(Dominant_Topic), group=as.factor(Dominant_Topic))) +
  geom_col() +
  geom_vline(xintercept = "2020-10", color="black") +
  scale_x_discrete(breaks=wy_scale, labels=wy_scale) +
  theme(axis.text.x = element_text(angle=45, hjust=1, vjust=0.5)) +
  theme_classic() +
  scale_fill_viridis(discrete = T, direction = -1, "Topic") +
  labs(
    title="Topic Relative Frequency",
    x="Week of the Year",
    y="Relative Frequency"
  ) +
  theme(
    text = element_text(family = "montserrat", size=20)
  )

ggsave(paste0(path_parent, "/plots/topic_rel_frequency.png"), width=7, height=4)

# Top Documents ----

sn %>%
  group_by(Dominant_Topic) %>%
  slice_max(Perc_Contribution, n=1)
