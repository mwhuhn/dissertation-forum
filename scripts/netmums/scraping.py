#!/usr/bin/env python3
# coding: utf-8

# Contains functions for scraping

import sqlite3
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup, NavigableString

def write_list(fn, l):
	with open(fn, 'a') as f:
		writer = csv.writer(f) 
		writer.writerow(l)

def read_list(fn):
	with open(fn, newline='') as f:
		reader = csv.reader(f)
		data = list(reader)
		return data

def create_connection(db_file):
	""" create a database connection to the SQLite database
		specified by the db_file
	:param db_file: database file
	:return: Connection object or None
	"""
	conn = None
	try:
		conn = sqlite3.connect(db_file)
	except sqlite3.Error as err:
		print(err)
	return conn

def requests_retry_session(retries=10, backoff_factor=.1, session=None):
	""" retry the request, backing off with longer rest each time
	:param retries: number of retries
	:param backoff_factor: each retry is longer by {backoff factor} * (2 ** ({number of total retries} - 1))
	:param session: persist session across requests
	:return session: session
	"""
	session = session or requests.Session()
	retry = Retry(
		total=retries,
		read=retries,
		connect=retries,
		backoff_factor=backoff_factor,
	)
	adapter = HTTPAdapter(max_retries=retry)
	session.mount('http://', adapter)
	session.mount('https://', adapter)
	return session

def get_soup(next_url):
	""" get the soup from the url
	:param next_url: string of next url to query
	:return soup: soup of url html
	:note: uses html5lib and not html because missing html returns errors
	"""
	headers = {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0'}
	try:
		res_next = requests_retry_session().get(next_url, headers=headers)
	except:
		return False
	soup = BeautifulSoup(res_next.content, "html5lib")
	return soup
