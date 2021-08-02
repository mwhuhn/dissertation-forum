# 3.1.1 Topic_Models-Compare_n_Topics

# Libraries ----

library(tidyverse)

# Path ----

path_parent <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))

# Load files ----

n_topics <- c(5,10,15,20,25,30,40,50)
fn <- paste0(path_parent, "/clean_data/dominant_topic_special-needs_all_%d.csv")

for (n in n_topics) {
  df <- read.csv(sprintf(fn, n))
  df <- df %>%
    select(-X) %>%
    rename_with(.fn = ~ paste0(.x, "_", n), .cols=Dominant_Topic:Perc_Contribution)
  if(n==5) {
    dom_topics <- df
  } else {
    dom_topics <- df %>%
      select(-text_clean) %>%
      full_join(dom_topics, by="message_id")
  }
}

table(dom_topics$Dominant_Topic_5)

rm(df)

dom_topics <- dom_topics %>%
  relocate(message_id, text_clean)

