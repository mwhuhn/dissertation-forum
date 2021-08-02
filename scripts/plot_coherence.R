# Load libraries ----
library(ggplot2)
library(dplyr)
# Load data ----
fn_w <- "~/Documents/dissertation/forum-sentiment/clean_data/coherence_special-needs.csv"
n_worker_n_topic <- read.csv(fn_w, header = F)
names(n_worker_n_topic) <- c("n_workers", "n_topics", "coherence", "perplexity")
fn_t <- "~/Documents/dissertation/forum-sentiment/clean_data/coherence_coherence-topics.csv"
n_topic <- read.csv(fn_t, header = F)
names(n_topic) <- c("sample", "n_topics", "coherence", "perplexity")

# Plot data ----
ggplot(
  n_worker_n_topic[n_worker_n_topic$n_topics<10,],
  aes(x=n_workers,
      y=coherence,
      group=n_topics,
      color=as.factor(n_topics))) +
  geom_line() +
  geom_point() +
  theme_classic() +
  labs(title = "Coherence by Workers and Topics",
       x = "Number of Workers",
       y = "Coherence",
       color = "Number\nof Topics")

# Average coherence by number of topics ----
fn_sn_p <- "~/Documents/dissertation/forum-sentiment/clean_data/coherence_special-needs_parents.csv"
sn_p <- read.csv(fn_sn_p, header = F)
names(sn_p) <- c("sample", "n_topics", "coherence", "perplexity")

sn_p_ave <- sn_p %>%
  group_by(n_topics) %>%
  summarize(ave_coherence = mean(coherence))
  
ggplot(sn_p) +
  geom_point(aes(x=as.factor(n_topics),
                 y=coherence,
                 group=n_topics),
             color="grey40") +
  geom_point(data=sn_p_ave,
             aes(x=as.factor(n_topics),
                 y=ave_coherence),
             color="firebrick1",
             size=3) +
  theme_classic() +
  labs(title = "Coherence by Topics",
       x = "Number of Topics",
       y = "Coherence",
       color = "Number\nof Topics")

n_topic %>%
  group_by(n_topics) %>%
  summarize(ave_coherence = mean(coherence)) %>%
  ggplot(
  aes(x=n_topics,
      y=ave_coherence)) +
  geom_line() +
  geom_point() +
  theme_classic() +
  labs(title = "Coherence by Topics",
       x = "Number of Topics",
       y = "Coherence")

