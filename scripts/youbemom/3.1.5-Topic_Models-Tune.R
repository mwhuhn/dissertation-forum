# 3.1.5 Tune Topic Models

# Libraries ----

library(tidyverse)

library(gganimate)
library(png)
library(gifski)
library(showtext)
# Path ----

path_parent <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))

# Load files ----

# sn_tune <- read.csv(paste0(path_parent,"/clean_data/a_b_t_tune_special-needs_parent_grid_2.csv"), header = F)
sn_tune <- read.csv(paste0(path_parent,"/clean_data/a_b_t_tune_special-needs_all_grid_1.csv"), header = F)
names(sn_tune) <- c("coherence", "perplexity", "n_topics", "alpha", "beta", "topic_n", "words")
resample <- read.csv(paste0(path_parent, "/clean_data/resample_10_topics_special-needs_parent.csv"), header = F)
names(resample) <- c("sample_n", "coherence", "topic_n", "words")

# Explore models ----

showtext_auto()
font_add_google("Montserrat", "montserrat")

resample %>%
  filter(topic_n==0) %>%
  ggplot(aes(x=coherence)) +
  geom_histogram(fill="springgreen1", color="springgreen4", binwidth = .004) +
  theme_classic() +
  labs(
    title="Coherence with 10 Topics",
    x="Coherence",
    y="",
    subtitle = "alpha = 0.7, beta = 0.1"
  ) +
  theme(
    text = element_text(family = "montserrat", size=20)
  )

ggsave(paste0(path_parent, "/plots/coherence_hist_10_topics.png"), width=5, height=4)

sn_tune %>%
  filter(topic_n==0) %>%
  slice_max(coherence, n=15) %>%
  select(coherence:beta)

sn_tune %>%
  filter(topic_n==0) %>%
  ggplot(aes(x=n_topics, y=coherence)) +
  geom_point(size=.3) +
  theme_classic() +
  labs(
    title="Coherence by Number of Topics",
    x="Number of Topics",
    y="Coherence"
  ) +
  theme(
    text = element_text(family = "montserrat", size=20)
    )

ggsave(paste0(path_parent, "/plots/coherence_by_topics.png"), width=5, height=4)

max_coherence <- sn_tune %>%
  group_by(n_topics) %>%
  filter(coherence == max(coherence))

max_coherence %>%
  filter(n_topics == 10) %>%
  select(words)


p <- sn_tune %>%
  filter(topic_n==0) %>%
  ggplot(aes(x=alpha, y=beta, color=coherence)) +
  geom_point(size=3, shape=15) +
  scale_color_distiller(palette = "Spectral") +
  theme_classic()  +
  theme(
    text = element_text(family = "montserrat", size=14)
  )

path_gif <- paste0(path_parent, "/plots/coherence_tuning.gif")

an <- p +
  transition_states(states = n_topics,
                    transition_length = 1,
                    state_length = 3) +
  labs(title="Topics: {closest_state}")

anim_save(path_gif, an, renderer=gifski_renderer(), duration=40, fps=10, width=5, height=4, units="in", res=100)



sn_tune %>%
  filter(n_topics == 10 & topic_n == 0) %>%
  ggplot(aes(x=alpha, y=beta, color=coherence)) +
  geom_point(size=3, shape=15) +
  scale_color_distiller(palette = "Spectral") +
  theme_classic()

ggsave(paste0(path_parent, "/plots/coherence_tuning_,",n,"_topics.png"), width=5, height=4)



