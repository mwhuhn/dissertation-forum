# 4.0.1-Track_Users-Plot_Network

# Libraries ----

library(tidyverse)
library(igraph)
library(tidygraph)
library(ggraph)

# read data ----

path_parent <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))
path_data <- paste0(path_parent, "/clean_data/network/forum_network.csv")
edges_df <- read.csv(path_data)


# make graph

g <- graph.data.frame(edges_df, directed=FALSE)
plot(g)
is_weighted(g)
tidy_g <- as_tbl_graph(g)

ggraph(tidy_g, layout = 'linear', circular = TRUE) + 
  geom_node_point() + 
  scale_size(range = c(2,10)) +
  geom_edge_arc(aes(width = weight), alpha = 0.25) +
  theme_graph()
