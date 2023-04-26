#!/usr/bin/env python3
# coding: utf-8

# Contains functions for lemmatizing text

# nltk
from nltk.corpus import wordnet as wn
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk import FreqDist

# gensim
from gensim import corpora, models
# data
import numpy as np
import pandas as pd
import string
import re
import emoji
import random
# paths
from pathlib import Path
from io import FileIO
# db
import sqlite3
from scraping import create_connection
# saving
import pickle

## File Locations

p = Path.cwd()
path_root = p.parents[1]

path_db = str(path_root / "database" / "netmums-merged.db")
path_clean_data = path_root / "clean_data" / "netmums"
path_lemma_pkl = str(path_clean_data / "lemmatized_text_{0}_{1}_{2}.pkl")
path_corpus_pkl = str(path_clean_data / "corpus_{0}_{1}_{2}.pkl")
path_dictionary_gensim = str(path_clean_data / "dictionary_{0}_{1}_{2}.gensim")
path_clean_text = str(path_clean_data / "clean_text_{0}_{1}_{2}.csv")
path_text_list = str(path_clean_data / "clean_text_list_{0}_{1}.csv")
path_freq_dist = str(path_clean_data / "freq_dist_{0}_{1}.pkl")

## Forums dictionary

forums = {
    "special-needs": 24,
    "work": 26,
    "travel": 25,
    "being-a-mum": 5,
    "coronavirus": 7,
    "education": 10,
    "legal": 16
}

## Regex

emoticon_lookaround = r'(?:(^)|(\s)|(?<=:\-\))|(?<=:\))|(?<=:\-\])|(?<=:\]:\-3)|(?<=:3)|(?<=:\->)|(?<=:>)|(?<=8\-\))|(?<=8\))|(?<=:\-\}})|(?<=:\}})|(?<=:o\))|(?<=:c\))|(?<=:\^\))|(?<==\])|(?<==\))|(?<=:\-D)|(?<=:D)|(?<=8\-D)|(?<=8D)|(?<=x\-D)|(?<=xD)|(?<=X\-D)|(?<=XD)|(?<==D)|(?<==3)|(?<=B\^D)|(?<=c:)|(?<=C:)|(?<=:\-\()|(?<=:\()|(?<=:\-c)|(?<=:c)|(?<=:\-<)|(?<=:<)|(?<=:\-\[)|(?<=:\[)|(?<=:\-\|\|)|(?<=>:\[)|(?<=:\{{)|(?<=:@)|(?<=:\()|(?<=;\()|(?<=:\'\-\()|(?<=:\'\()|(?<=:\'\-\))|(?<=:\'\))|(?<=D\-\':)|(?<=D:<)|(?<=D:)|(?<=D8)|(?<=D;)|(?<=D=)|(?<=DX)|(?<=:\-O)|(?<=:O)|(?<=:\-o)|(?<=:o)|(?<=:\-0)|(?<=8\-0)|(?<=>:O)|(?<=O_O)|(?<=o-o)|(?<=O_o)|(?<=o_O)|(?<=o_o)|(?<=O-O)|(?<=:\-\*)|(?<=:\*)|(?<=:x)|(?<=;\-\))|(?<=;\))|(?<=\*\-\))|(?<=\*\))|(?<=;\-\])|(?<=;\])|(?<=;\^\))|(?<=:\-,)|(?<=;D)|(?<=:\-P)|(?<=:P)|(?<=X\-P)|(?<=XP)|(?<=x\-p)|(?<=xp)|(?<=:\-p)|(?<=:p)|(?<=:\-Þ)|(?<=:Þ)|(?<=:\-þ)|(?<=:þ)|(?<=:\-b)|(?<=:b)|(?<=d:)|(?<==p)|(?<=>:P)|(?<=:\-/)|(?<=:/)|(?<=:\-\.)|(?<=>:\\)|(?<=>:/)|(?<=:\\)|(?<==/)|(?<==\\)|(?<=:L)|(?<==L)|(?<=:S)|(?<=:\-\|)|(?<=:\|)|(?<=<3)|(?<=<\\3)|(?<=\\[oO]/)){}(?=($|\s|:\-\)|:\)|:\-\]|:\]:\-3|:3|:\->|:>|8\-\)|8\)|:\-\}}|:\}}|:o\)|:c\)|:\^\)|=\]|=\)|:\-D|:D|8\-D|8D|x\-D|xD|X\-D|XD|=D|=3|B\^D|c:|C:|:\-\(|:\(|:\-c|:c|:\-<|:<|:\-\[|:\[|:\-\|\||>:\[|:\{{|:@|:\(|;\(|:\'\-\(|:\'\(|:\'\-\)|:\'\)|D\-\':|D:<|D:|D8|D;|D=|DX|:\-O|:O|:\-o|:o|:\-0|8\-0|>:O|O_O|o-o|O_o|o_O|o_o|O-O|:\-\*|:\*|:x|;\-\)|;\)|\*\-\)|\*\)|;\-\]|;\]|;\^\)|:\-,|;D|:\-P|:P|X\-P|XP|x\-p|xp|:\-p|:p|:\-Þ|:Þ|:\-þ|:þ|:\-b|:b|d:|=p|>:P|:\-/|:/|:\-\.|>:\\|>:/|:\\|=/|=\\|:L|=L|:S|:\-\||:\||<3|<\\3|\\[oO]/))'

## Functions

def save_clean_text(df, forum="all", group="all", id_type="thread_id",):
	df.to_csv(path_clean_text.format(forum, group, id_type), index=False)

def process_data(forum="all", group="all", id_type="thread_id", per=1, n_chunks=1):
	if Path(path_clean_text.format(forum, group, id_type)).exists():
		df = pd.read_csv(path_clean_text.format(forum, group, id_type))
		print("found clean text")
	else:
		print("creating new clean text")
		df = load_data(forum, group, per)
		df = clean_data(df)
		save_clean_text(df, forum, group)
		print("saved clean text")
	print("text to list")
	df['bigrams'] = text_to_list(df['text_clean'], n_chunks)
	if id_type == "thread_id":
		print("making bigrams")
		df['bigrams'] = make_bigrams(df['bigrams'], n_chunks)
		print("grouping by thread_id")
		text = df[['thread_id','bigrams']].groupby(['thread_id'])['bigrams'].sum().tolist()
	else:
		print("making bigrams")
		text = make_bigrams(df['bigrams'], n_chunks)
	print("saving data")
	save_data(text, forum, group, id_type, n_chunks)

def process_data_df(df, savename, n_chunks):
	df = clean_data(df)
	df.to_csv(str(path_clean_data / "clean_text_{}".format(savename)), index=False)
	print("saved clean text")
	print("text to list")
	df['bigrams'] = text_to_list(df['text_clean'], n_chunks)
	print("making bigrams")
	df['bigrams'] = make_bigrams(df['bigrams'], n_chunks)
	print("grouping by thread_id")
	text = df[['thread_id','bigrams']].groupby(['thread_id'])['bigrams'].sum().tolist()
	print("saving data")
	save_data_df(text, savename, n_chunks)

def save_text_list(forum="all", group="all", force=False):
	if not Path(path_text_list.format(forum, group)).exists() or force:
		df = pd.read_csv(path_clean_text.format(forum, group))
		text = text_to_list(df['text_clean'], 1)
		with open(path_text_list.format(forum, group), 'wb') as f:
			pickle.dump(text, f)

def read_text_list(forum="all", group="all"):
	with open(path_text_list.format(forum, group), 'rb') as f:
		return pickle.load(f)

def get_freq_dist(forum="all", group="all", force=False):
	if not Path(path_freq_dist.format(forum, group)).exists() or force:
		save_text_list(forum, group)
		text = read_text_list(forum, group)
		flat_text = [word for doc in text for word in doc]
		freq_dist = FreqDist(flat_text)
		with open(path_freq_dist.format(forum, group), 'wb') as f:
			pickle.dump(freq_dist, f)
	else:
		with open(path_freq_dist.format(forum, group), 'rb') as f:
			freq_dist = pickle.load(f)
	return freq_dist

def load_data(forum="all", group="all", per=1):
	conn = create_connection(path_db)
	if per == 1: # only for toddler
		sql = gen_sql(forum, group)
	else:
		sql_temp = gen_samp_sql_per(forum)
		temp = pd.read_sql_query(sql_temp, conn)
		temp = temp.sample(frac=per, random_state=232)
		temp.to_sql('temp', conn, if_exists='replace', index=False)
		sql = gen_sql_per(group)
	df = pd.read_sql_query(sql, conn)
	conn.close()
	return df



def save_data(text, forum="all", group="all", id_type="thread_id", n_chunks=1):
	pickle.dump(text, open(path_lemma_pkl.format(forum, group, id_type), 'wb'))
	if n_chunks == 1:
		dictionary = corpora.Dictionary(text)
	else:
		length = len(text)
		size = length // n_chunks + (length % n_chunks > 0) # round up
		for i, grp in enumerate(chunk_list(text, size)):
			if i == 0:
				dictionary = corpora.Dictionary(grp)
			else:
				dictionary.add_documents(grp)
	dictionary.save(FileIO(path_dictionary_gensim.format(forum, group, id_type), "wb"))
	if n_chunks > 1:
		length = len(text)
		size = length // n_chunks + (length % n_chunks > 0) # round up
		corpus = []
		for grp in chunk_list(text, size): # iterable
			corpus = corpus + [dictionary.doc2bow(g) for g in grp]
	else:
		corpus = [dictionary.doc2bow(t) for t in text]
	pickle.dump(corpus, open(path_corpus_pkl.format(forum, group, id_type), 'wb'))

def save_data_df(text, savename, n_chunks=1):
	pickle.dump(text, open(str(path_clean_data / "lemmatized_text_{}.pkl".format(savename)), 'wb'))
	if n_chunks == 1:
		dictionary = corpora.Dictionary(text)
	else:
		length = len(text)
		size = length // n_chunks + (length % n_chunks > 0) # round up
		for i, grp in enumerate(chunk_list(text, size)):
			if i == 0:
				dictionary = corpora.Dictionary(grp)
			else:
				dictionary.add_documents(grp)
	dictionary.save(FileIO(str(path_clean_data / "dictionary_{}.gensim".format(savename)), "wb"))
	if n_chunks > 1:
		length = len(text)
		size = length // n_chunks + (length % n_chunks > 0) # round up
		corpus = []
		for grp in chunk_list(text, size): # iterable
			corpus = corpus + [dictionary.doc2bow(g) for g in grp]
	else:
		corpus = [dictionary.doc2bow(t) for t in text]
	pickle.dump(corpus, open(str(path_clean_data / "corpus_{}.pkl".format(savename)), 'wb'))

def chunk_list(l, n):
	"""Yield successive n-sized chunks from lst."""
	for i in range(0, len(l), n):
		yield l[i:i + n]

def clean_text(text):
	""" cleans the input text of punctuation, extra
		spaces, and makes letters lower case
	:param text: text (= title + body here)
	:return clean: clean text
	"""
	try:
		clean = "".join([t for t in text if t not in string.punctuation])
	except:
		print(text)
	clean = re.sub(" +", " ", clean)
	clean = clean.strip()
	clean = clean.lower()
	return clean

def remove_stopwords(text):
	""" remove all stop words from the text
		using stopwords from nltk.corpus
	:param text: text with stopwords
	:return words: text without stopwords
	"""
	words = [w for w in text if w not in stopwords.words('english')]
	return words

def encode_utf8(text):
	words = [w.encode() for w in text]
	return words

def get_lemma(word):
	lemma = wn.morphy(word)
	if lemma is None:
		return word
	else:
		return lemma

def lemmatize(text):
	lemmas = [get_lemma(w) for w in text]
	return lemmas

def replace_lonely_numbers(df):
	lonely_number_pattern = r'^[0-9]+$'
	regex_pat = re.compile(lonely_number_pattern, flags=re.IGNORECASE)
	df['text_clean'] = df['text_clean'].str.replace(regex_pat, "")
	df['text_clean'] = df['text_clean'].str.strip()
	return df

def replace_email(df):
	email_pattern = r'([-a-zA-Z0-9_\.\+]+@[-a-zA-Z0-9]+\.[-a-zA-Z0-9\.]+)'
	regex_pat = re.compile(email_pattern, flags=re.IGNORECASE)
	df['text_clean'] = df['text_clean'].str.replace(regex_pat, "")
	df['text_clean'] = df['text_clean'].str.strip()
	return df

def replace_emoji(df):
	# See: https://en.wikipedia.org/wiki/List_of_emoticons
	emoticons = [
		[r'(:\-\)|:\)|:\-\]|:\]:\-3|:3|:\->|:>|8\-\)|8\)|:\-\}|:\}|:o\)|:c\)|:\^\)|=\]|=\))',' emojismile '],
		[r'(:\-D|:D|8\-D|8D|x\-D|xD|X\-D|XD|=D|=3|B^D|c:|C:)',' emojilaughing '],
		[r'(:\-\(|:\(|:\-c|:c|:\-<|:<|:\-\[|:\[|:\-\|\||>:\[|:\{|:@|:\(|;\()',' emojifrowning '],
		[r'(:\'\-\(|:\'\()',' emojicry '],
		[r'(:\'\-\)|:\'\))',' emojijoy '],
		[r'(D\-\':|D:<|D:|D8|D;|D=|DX)',' emojianguished '],
		[r'(:\-O|:O|:\-o|:o|:\-0|8\-0|>:O|O_O|o-o|O_o|o_O|o_o|O-O)',' emojiopenmouth '],
		[r'(:\-\*|:\*|:x)',' emojikissing '],
		[r'(;\-\)|;\)|\*\-\)|\*\)|;\-\]|;\]|;\^\)|:\-,|;D)',' emojiwink '],
		[r'(:\-P|:P|X\-P|XP|x\-p|xp|:\-p|:p|:\-Þ|:Þ|:\-þ|:þ|:\-b|:b|d:|=p|>:P)',' emojistuckouttongue '],
		[r'(:\-/|:/|:\-\.|>:\\|>:/|:\\|=/|=\\|:L|=L|:S)',' emojiconfused '],
		[r'(:\-\||:\|)',' emojiexpressionless '],
		[r'(<3)',' emojiheart '],
		[r'(<\\3)',' emojibrokenheart '],
		[r'(\\[oO]/)',' emojicheer ']
	]
	for grp in emoticons:
		pattern = emoticon_lookaround.format(grp[0])
		df['text_clean'] = df['text_clean'].str.replace(re.compile(pattern), grp[1])
	df['text_clean'] = df['text_clean'].apply(lambda x: emoji.demojize(x, delimiters=(" emoji", " ")))
	return df

def drop_nonalpha(df):
	alpha_pattern = r'[a-zA-Z]'
	regex_pat = re.compile(alpha_pattern, flags=re.IGNORECASE)
	df['has_alpha'] = df['text_clean'].str.contains(regex_pat)
	df.loc[~df['has_alpha'], 'text_clean'] = ""
	df['text_clean'].replace('', np.nan, inplace=True)
	df.dropna(subset=['text_clean'], inplace=True)
	df.drop('has_alpha', axis=1, inplace=True)
	return df

def make_bigrams(text, n_chunks=1):
	bigram = models.Phrases(text, min_count=5)
	bigram_mod = models.phrases.Phraser(bigram)
	result = []
	for grp in np.array_split(text, n_chunks):
		grp = grp.tolist()
		result = result + [bigram_mod[g] for g in grp]
	return result


def clean_data(df):
	df = replace_emoji(df)
	df = replace_email(df)
	df = replace_lonely_numbers(df)
	df = drop_nonalpha(df)
	df['text_clean'] = df['text_clean'].apply(clean_text)
	return df

def text_to_list(text, n_chunks=1):
	tokenizer = RegexpTokenizer(r'\w+')
	result = []
	for grp in np.array_split(text, n_chunks):
		grp = grp.apply(tokenizer.tokenize)
		grp = grp.apply(remove_stopwords)
		grp = grp.apply(lemmatize)
		grp = grp.tolist()
		result = result + grp
	return result

def gen_sql(forum="all", group="all"):
	sql = '''
		SELECT
			p.thread_id AS thread_id,
			text.post_id AS post_id,
			text.text_clean AS text_clean
		FROM text
		LEFT JOIN posts AS p
		ON text.post_id = p.id
		LEFT JOIN threads AS t
		ON t.id=p.thread_id
		LEFT JOIN subforums AS s
		ON s.id=t.subforum_id
		LEFT JOIN forums AS f
		ON f.id=s.forum_id
		WHERE
	'''
	if forum != "all":
		sql = sql + ''' f.id={} AND'''.format(forums[forum])
	if group=="parent":
		sql = sql + ''' p.post_count=1 AND'''
	elif group=="child":
		sql = sql + ''' p.post_count!=1 AND'''
	sql = sql + ''' text.text_clean!="" '''
	return sql

def gen_samp_sql_per(forum="all"):
	sql = ''' SELECT t.id AS thread_id FROM threads AS t '''
	if forum!="all":
		sql = sql + '''
			LEFT JOIN subforums AS s
			ON s.id=t.subforum_id
			LEFT JOIN forums AS f
			ON f.id=s.forum_id
			WHERE f.id={}
		'''.format(forums[forum])
	return sql

def gen_sql_per(group="all"):
	sql = '''
		SELECT
			p.thread_id AS thread_id,
			text.post_id AS post_id,
			text.text_clean AS text_clean
		FROM text
		LEFT JOIN posts AS p
		ON text.post_id = p.id
		WHERE p.thread_id IN (SELECT thread_id FROM temp) AND
	'''
	if group=="parent":
		sql = sql + ''' p.post_count=1 AND'''
	elif group=="child":
		sql = sql + ''' p.post_count!=1 AND'''
	sql = sql + ''' text.text_clean<>"" '''
	return sql
