#!/usr/bin/env python
# coding: utf-8

# # Lemmatize Forum Text
# Cleans the data for topic modelling

# my functions
from lemmatize import *




## Process Data

subforum = "tween-teen"
group = "all"
n_chunks = 1
text = process_data(subforum, group)
text = text_to_list(text, n_chunks)
text = make_bigrams(text, n_chunks)
save_data(text, subforum, group, n_chunks)
