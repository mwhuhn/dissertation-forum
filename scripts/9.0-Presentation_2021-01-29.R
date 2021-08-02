# Presentation 1/29/2021

# Libraries ----

library(tidyverse)
library(RSQLite)
library(DBI)

# Path ----

path_parent <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))

# Connect to db ----

fn <- paste0(path_parent, "/database/youbemom-merged.db")
conn <- dbConnect(SQLite(), fn)

## . SQL ----

sql.year.subforum.all <- "
  SELECT substr(date_created, 1, 4) AS year, subforum, COUNT(message_id) AS n
  FROM posts
  GROUP BY subforum, year
  ORDER BY subforum, year;
"

sql.year.subforum.no.spam <- "
  SELECT substr(p.date_created, 1, 4) AS year, p.subforum AS subforum, COUNT(p.message_id) AS n
  FROM text AS t
  JOIN posts AS p
  ON t.message_id = p.message_id
  WHERE t.text_clean<>'' AND t.probable_spam=0
  GROUP BY subforum, year
  ORDER BY subforum, year;
"

sql.year.subforum.spam <- "
  SELECT substr(p.date_created, 1, 4) AS year, p.subforum AS subforum, COUNT(p.message_id) AS n
  FROM text AS t
  JOIN posts AS p
  ON t.message_id = p.message_id
  WHERE t.probable_spam=1
  GROUP BY subforum, year
  ORDER BY subforum, year;
"

# Get Queries ----

year.subforum.all <- dbGetQuery(conn, sql.year.subforum.all)
year.subforum.no.spam <- dbGetQuery(conn, sql.year.subforum.no.spam)
year.subforum.spam <- dbGetQuery(conn, sql.year.subforum.spam)

# Clean Data ----
group_other <- function(df) {
  df$subforum[df$subforum != "toddler" & df$subforum != "special-needs"] <- "other"
  df <- df %>%
    group_by(subforum, year) %>%
    summarise(
      count = sum(n),
      .groups = "drop_last"
    ) %>%
    filter(year <= 2020)
  return(df)
}

year.subforum.all <- group_other(year.subforum.all)
year.subforum.no.spam <- group_other(year.subforum.no.spam)
year.subforum.spam <- group_other(year.subforum.spam)

year.subforum.all$type <- "all"
year.subforum.no.spam$type <- "clean"
year.subforum.spam$type <- "spam"

all <- rbind(year.subforum.all, year.subforum.no.spam, year.subforum.spam)

# Rates ----

all.wide <- all %>%
  group_by(subforum, type) %>%
  summarize(
    n = sum(count),
    .groups = "drop_last"
  ) %>%
  pivot_wider(names_from = type, values_from = n) %>%
  mutate(
    spam.rate = spam / all,
    spam.rate.per = round(100*spam.rate, 3)
  )

# Compare topic overlap ----

resample <- read.csv(paste0(path_parent, "/clean_data/resample_10_topics_special-needs_parent.csv"), header = F)
names(resample) <- c("sample_n", "coherence", "topic_n", "words")

split_topics <- function(topic) {
  topic <- gsub("\"", "", topic)
  topic <- strsplit(topic, " \\+ ")[[1]]
  topic <- strsplit(topic, "\\*")
  return(topic)
}

topics_df <- function(topic) {
  listHolder <- split_topics(topic)
  topics <- as.data.frame(do.call(rbind, listHolder))
  names(topics) <- c("coherence", "word")
  return(topics)
}

words <- do.call(rbind, map(resample$words, topics_df))
words$word_n <- rep(1:10, 1010)
words$topic_n <- rep(rep(1:10, each=10), 101)
words$group_n <- rep(1:101, each=100)
word_counts <- words %>%
  group_by(word) %>%
  summarise(
    n = n(),
    .groups = "drop_last"
  )

# wordcloud(words = word_counts$word,
#           freq = word_counts$n,
#           min.freq = 1,
#           max.words=200,
#           random.order=FALSE,
#           rot.per=0.35,
#           colors=brewer.pal(8, "RdYlBu"))

overlap <- words %>%
  group_by(group_n, word) %>%
  summarise(
    n = n(),
    .groups = "drop_last"
  ) %>%
  select(group_n, n) %>%
  group_by(group_n) %>%
  summarise(
    degree_overlap = sum(n) / ,
    count_overlap = sum(n > 1),
    .groups = "drop_last"
  ) %>%
  left_join(
    resample %>%
      filter(topic_n == 0) %>%
      select(coherence) %>%
      mutate(
        group_n = row_number()
      ),
    by="group_n"
  )

