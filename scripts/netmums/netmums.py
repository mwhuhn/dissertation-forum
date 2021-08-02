#!/usr/bin/env python3
# coding: utf-8

# Contains functions for scraping Netmums forum posts

import re
import sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup, Doctype
from dateutil.parser import parse
from scraping import *
import json
import emoji
import time

## Regex

skip = re.compile(r'(product tests|becoming a mum|faq|research requests)', re.IGNORECASE)
sent_from = re.compile(r'sent\s+from\s+my\s+[a-z0-9\-]*\s+using\s+netmums\s+mobile\s+app', re.IGNORECASE)
shortened_url = re.compile(r'\[\.\.\.\]', re.IGNORECASE)
extra_spaces = re.compile(r'\s+')
url_id = re.compile(r'#post([0-9]+)')

## Functions for creating and writing to the database

def set_up_posts_db(conn):
	""" sets up tables in netmums database to be merged later
		if the table doesn't exists, create it
	:param conn: database connection
	:return: nothing
	"""
	cur = conn.cursor()
	cur.executescript('''
		CREATE TABLE IF NOT EXISTS users(
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			name TEXT,
			user_url TEXT
		);
		CREATE TABLE IF NOT EXISTS posts(
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			thread_id INTEGER,
			post_count INTEGER,
			post_id TEXT,
			user_url TEXT,
			date_created TEXT,
			date_recorded TEXT,
			body TEXT,
			version INTEGER
		);
		CREATE TABLE IF NOT EXISTS quotes(
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			thread_id TEXT,
			post_count INTEGER,
			quoting_id TEXT,
			quoted_id TEXT,
			quoted_user TEXT,
			quoted_text TEXT,
			citation_n INTEGER
		);
		CREATE TABLE IF NOT EXISTS links(
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			thread_id TEXT,
			post_count INTEGER,
			post_id TEXT,
			link_count INTEGER,
			link_text TEXT,
			link_url TEXT
		);
	''')

def set_up_merged_db(conn):
	""" sets up tables in netmums database
		if the table doesn't exists, create it
	:param conn: database connection
	:return: nothing
	"""
	cur = conn.cursor()
	cur.executescript('''
		CREATE TABLE IF NOT EXISTS forums (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			url TEXT,
			name TEXT
		);
		CREATE TABLE IF NOT EXISTS subforums (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			url TEXT,
			name TEXT,
			forum_id INTEGER
		);
		CREATE TABLE IF NOT EXISTS users(
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			name TEXT,
			user_url
		);
		CREATE TABLE IF NOT EXISTS threads(
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			url TEXT,
			subject TEXT,
			subforum_id INTEGER
		);
		CREATE TABLE IF NOT EXISTS posts(
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			thread_id INTEGER,
			post_count INTEGER,
			post_id TEXT,
			user_url TEXT,
			date_created TEXT,
			date_recorded TEXT,
			body TEXT,
			version INTEGER
		);
		CREATE TABLE IF NOT EXISTS quotes(
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			thread_id TEXT,
			post_count INTEGER,
			quoting_id TEXT,
			quoted_id TEXT,
			quoted_user TEXT,
			quoted_text TEXT,
			citation_n INTEGER
		);
		CREATE TABLE IF NOT EXISTS links(
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			thread_id TEXT,
			post_count INTEGER,
			post_id TEXT,
			link_count INTEGER,
			link_text TEXT,
			link_url TEXT
		);
	''')

def write_citation(cur, thread_id, post_count, quoting_id, quoted_id, quoted_user, quoted_text, citation_n):
	""" writes citation ids to the quotes table,
		links quoted posts to the quoting posts
	:param thread_id: id of thread, linked to threads table
	:param post_count: count of post in thread
	:param quoting_id: id of post quoting another post
	:param quoted_id: id of post being quoted
	:param quoted_user: user being quoted
	:param quoted_text: text of quote
	:return: nothing
	"""
	parsed = (thread_id, post_count, quoting_id, quoted_id, quoted_user, quoted_text, citation_n)
	sql = '''
		INSERT INTO quotes(thread_id, post_count, quoting_id, quoted_id, quoted_user, quoted_text, citation_n)
		VALUES(?,?,?,?,?,?,?)
	'''
	cur.execute(sql, parsed)

def write_post(cur, thread_id, post_count, post_id, user_url, date_created, body, version):
	""" writes post information to posts table
	:param thread_id: id of thread, linked to threads table
	:param post_count: count of post in thread
	:param post_id: id scraped from post, linked to quotes table
	:param user_url: user url, linked to users table
	:param date_created: date post was created
	:param date_recorded: date post was recorded
	:param body: text body of post
	:return: nothing
	"""
	date_recorded = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	parsed = (thread_id, post_count, post_id, user_url, date_created, date_recorded, body, version)
	sql = '''
		INSERT INTO posts(thread_id, post_count, post_id, user_url, date_created, date_recorded, body, version)
		VALUES(?,?,?,?,?,?,?,?)
	'''
	cur.execute(sql, parsed)

def write_user(cur, name, user_url):
	""" write user information to users table
	:param name: string of name
	:param user_url: url of profile
	:return: nothing
	:note: does not check for duplicates,
		will delete duplicates after scrape
		to make writing quicker
	"""
	parsed = (name, user_url)
	sql = '''
		INSERT INTO users(name, user_url)
		VALUES(?,?)
	'''
	cur.execute(sql, parsed)

def write_link(cur, thread_id, post_count, post_id, link_count, link_text, link_url):
	""" write link information to links table
	:param thread_id: id of thread, linked to threads table
	:param post_count: count of post in thread
	:param post_id: id of post with link
	:param link_count: link number within post
	:param link_text: text of link
	:param link_url: href of link
	:return: nothing
	"""
	parsed = (thread_id, post_count, post_id, link_count, link_text, link_url)
	sql = '''
		INSERT INTO links(thread_id, post_count, post_id, link_count, link_text, link_url)
		VALUES(?,?,?,?,?,?)
	'''
	cur.execute(sql, parsed)

## Functions for scraping Netmums

### Forum

def keep_forum(name):
	""" skips or keeps forum based on forum name
	:param name: forum name
	:return: bool keep or skip
	"""
	if re.search(skip, name):
		return False
	else:
		return True

### Threads

def scrape_threads(cur, row):
	""" scrape information on threads from list of subforums
	:param row: row selected from subforum table
	:return: nothing
	"""
	page = 1
	(subforum_id, url) = row
	next_url = url + "index{}.html"
	soup = get_soup(url)
	not_end = True
	while not_end:
		if soup.find("a", {"class": "cCatTopic"}):
			for thread in soup.find_all("td", {"class": "sujetCase3"}):
				link = thread.find("a", {"class": "cCatTopic"})
				parsed = (link['href'], link.get_text(), subforum_id)
				sql = '''
					INSERT INTO threads(url,subject,subforum_id)
					VALUES(?,?,?)
				'''
				cur.execute(sql, parsed)
			page += 1
			soup = get_soup(next_url.format(str(page)))
		else:
			not_end = False

### Posts

def doctype_html(soup):
	items = [item for item in soup.contents if isinstance(item, Doctype)]
	if items:
		return items[0]=="html"
	else:
		return None

def get_next_url(soup):
	""" get the next page url in the thread, else False
	:param soup: soup of the current page
	:return: the next url or False, if no next url
	"""
	next_button = soup.find("div", {"class":"pagepresuiv_next"})
	link = next_button.find("a")
	if link:
		next_url = link.get('href')
		return next_url
	return False

def get_user_old(cur, soup):
	""" get the user from the post using the old interface
	:param soup: soup of the current page
	:return: the post's user name, data user id, and url
	:note: writes user to database
	"""
	user = soup.find("span", {"class": "CF_user_mention"}, recursive=True)
	if not user:
		user_url = "Anonymous"
	else:
		user_name = user.get_text()
		avatar = soup.find("div", {"class","avatar_center"})
		img = avatar.find("img")
		user_url = img.get("alt")
		write_user(cur, user_name, user_url)
	return user_url

def get_user_new(cur, post):
	""" get the user from the post using the new interface
	:param post: json of the current post
	:return: the post's user name, data user id, and url
	:note: writes user to database
	"""
	if post["user"]["anonymized"]:
		user_url = "Anonymous"
	else:
		user_url = post["user"]["userSlug"]
		user_name = post["user"]["pseudo"]
		write_user(cur, user_name, user_url)
	return user_url

def get_date_old(soup):
	""" get the date from the post using the old interface
	:param soup: soup of the current page
	:return: the post's date created formatted Y-M-D H:MP
	"""
	date = soup.find("span", {"class": "topic_posted"}, recursive=True)
	time = re.sub("(Posted on\s|at\s)", "", date.get_text()).strip()
	time = re.sub("\s+", " ", time)
	time = datetime.strptime(time, "%d-%m-%Y %I.%M%p" ).strftime("%Y-%m-%d %I:%M%p")
	return time

def get_date_new(post):
	""" get the date from the post using the new interface
	:param post: json of the current post
	:return: the post's date created formatted Y-M-D H:MP
	"""
	return datetime.strptime(post["date"], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %I:%M%p")

def strip_text(text):
	""" strips the text of extra spaces and "sent from" text
	:param text: text to clean
	:return: striped text
	"""
	clean = re.sub(sent_from, "", text).strip()
	return re.sub(extra_spaces, " ", clean)

def is_shortened_url(text):
	""" checks if the url in the text is a shorted url "[...]"
	:param text: text of url
	:return: bool
	"""
	if re.search(shortened_url, text):
		return True
	else:
		return False

def get_post_id(post):
	""" finds the id of the post from the post content
	:param post: post content div
	:return: id of post
	"""
	post_id = re.sub("para", "", post['id'])
	return post_id

def get_citations_old(cur, post, thread_id, post_count, post_id):
	""" finds the citations in a post using the old interface
	:param post: post content div
	:param post_id: post id
	:return: length of citations list
	:note: writes citations to quotes table
	"""
	citations = post.find_all("span", {"itemprop": "citation"})
	for i, cite in enumerate(citations):
		cite_url = cite.find("span", {"itemprop": "url"}).get_text()
		cite_id = id_from_url(cite_url)
		cited_user = cite.find("span", {"itemprop": "author"}).get_text()
		write_citation(cur, thread_id, post_count, post_id, cite_id, cited_user, "", i + 1)
	return len(citations)

def process_citation_new(cur, cite, thread_id, post_count, post_id, citation_n):
	""" finds the citations in a post using the new interface
	:param cite: citation content json
	:param post_id: post id
	:return: length of citations list
	:note: writes citations to quotes table
	"""
	if cite["quotedPostAuthor"]["anonymized"]:
		cited_user = "Anonymous"
	else:
		cited_user = cite["quotedPostAuthor"]["pseudo"]
	content = clean_text_new(cur, cite["quotedPostContent"], thread_id, post_count, post_id, write=False)
	write_citation(cur, thread_id, post_count, post_id, "", cited_user, content, citation_n)

def id_from_url(text):
	""" finds the id of the post from the url
	:param text: text of url
	:return: id of quoted post or empty string if not found
	"""
	quote_id = re.search(url_id, text)
	if quote_id:
		return quote_id.group(1)
	return ""

def replace_emojis(soup):
	""" finds any wysiwyg emojis in the body and
		replaces them with text representation
	:param div: post body div
	:return: div cleaned of emojis
	"""
	imgs = soup.find_all("img", {"class":"wysiwyg_smiley"})
	for img in imgs:
		if img.has_attr("title"):
			img.string = " " + img.get("title") + " "
		else:
			img.string = " ::emoji:: "
	return soup

def replace_breaks(soup):
	""" finds and replaces any breaks with new lines
	:param soup: soup with breaks
	:return: soup without breaks
	"""
	for br in soup('br'):
		br.replace_with('\n')
	return soup

def replace_hrefs(cur, soup, thread_id, post_count, post_id, write=True):
	""" finds any links in the body with shortened urls
		and replaces them with the full url
	:param soup: post body
	:return: soup cleaned of shortened urls
	"""
	hrefs = soup.find_all("a")
	for i, href in enumerate(hrefs):
		link_url = href.get("href")
		link_text = href.get_text()
		link_count = i + 1
		href.string = "::link_{}::".format(link_count)
		if write:
			write_link(cur, thread_id, post_count, post_id, link_count, link_text, link_url)
	return soup

def clean_text_new(cur, post_content, thread_id, post_count, post_id, write=True):
	""" cleans the text from the new interface
	:param text: text to clean
	returns: clean text
	"""
	content = BeautifulSoup(post_content, "html5lib")
	content = replace_breaks(content)
	content = replace_emojis(content)
	content = replace_hrefs(cur, content, thread_id, post_count, post_id, write)
	content = content.get_text()
	return strip_text(content)

def get_text_no_cite(cur, post, thread_id, post_count, post_id):
	""" finds the text from posts with no citations
	:param post: post soup
	:return: cleaned body text
	"""
	div = post.find("div")
	div = replace_emojis(div)
	div = replace_hrefs(cur, div, thread_id, post_count, post_id)
	text = ""
	for child in div.children:
		if isinstance(child, NavigableString):
			text = text + " " + child
		else:
			text = text + " " + child.get_text(separator=" ")
	text = emoji.demojize(text, delimiters=(" ::", ":: "))
	return strip_text(text)

def get_text_cite(cur, post, thread_id, post_count, post_id):
	""" finds the text from posts with citations
	:param post: post soup
	:return: cleaned body text
	"""
	div = post.find("div")
	child_divs = div.find_all("div")
	for child in child_divs:
		child.replace_with('\n')
	div = replace_emojis(div)
	div = replace_hrefs(cur, div, thread_id, post_count, post_id)
	text = div.get_text()
	text = emoji.demojize(text, delimiters=(" ::", ":: "))
	return strip_text(text)

def get_posts(soup):
	script = soup.find("script", {"id":"__NEXT_DATA__"})
	script = script.get_text()
	script = json.loads(script)
	return script["props"]["pageProps"]["initialReduxState"]["currentThread"]["currentThread"]["pagePosts"]

def is_error(soup):
	if soup.find("h2", {"data-translate":"what_happened"}) or soup.find("span", {"class":"cf-error-code"}):
		return True
	return False

def scrape_posts(conn, row):
	""" scrape information on posts from threads table
	:param row: row from threads table
	:return: nothing
	"""
	cur = conn.cursor()
	(thread_id, url) = row
	next_url = url
	post_count = 0
	page = 0
	while next_url:
		page_post_count = 0
		page += 1
		soup = get_soup(next_url)
		error_count = 0
		skip_row = False
		while is_error(soup):
			error_count += 1
			time.sleep(10)
			soup = get_soup(next_url)
			if error_count > 5:
				print("skip next url:", next_url)
				write_list("errors.csv", [thread_id,next_url])
				skip_row = True
				break
		if not skip_row:
			doctype_soup = doctype_html(soup)
			if doctype_soup:
				# page from new version of forum
				error404 = soup.find("div", {"class":"error-404"})
				faq = soup.find("meta", {"content":"Netmums FAQs"})
				if error404:
					break
				if faq:
					break
				else:
					if page == 1:
						canonical_link = soup.find("link", {"rel":"canonical"})["href"]
						if canonical_link != next_url: # redirected page
							url = canonical_link
					next_url = re.sub(r'.html', '-{}.html'.format(page + 1), url)
					script = soup.find("script", {"id":"__NEXT_DATA__"})
					try:
						script = script.get_text()
						script = json.loads(script)
						posts = script["props"]["pageProps"]["initialReduxState"]["currentThread"]["currentThread"]["pagePosts"]
					except:
						print(soup)
						posts = []
						write_list("errors.csv", [thread_id,next_url])
					for post in posts:
						post_count += 1
						user_url = get_user_new(cur, post)
						date_created = get_date_new(post)
						post_id = post["id"]
						text = clean_text_new(cur, post["content"], thread_id, post_count, post_id)
						citations = post["quotedPosts"]
						for i, cite in enumerate(citations):
							process_citation_new(cur, cite, thread_id, post_count, post_id, i + 1)
						write_post(cur, thread_id, post_count, post_id, user_url, date_created, text, 1)
						conn.commit()
			elif doctype_soup is None:
				# no next page for new version of forum
				next_url = False
			else:
				# page from old version of forum
				error404 = soup.find("div", {"class":"error-404"})
				if error404:
					break
				else:
					next_url = get_next_url(soup) # returns False if no next url
					cases = soup.find_all("div", {"class":"md-topic_post"})
					for case in cases:
						page_post_count += 1
						if page > 1 and page_post_count == 1: # skip dup posts on top of page
							continue
						else:
							post_count += 1
							user_url = get_user_old(cur, case)
							date_created = get_date_old(case)
							post = case.find("div", {"class": "post_content"})
							post_id = get_post_id(post)
							len_citations = get_citations_old(cur, post, thread_id, post_count, post_id)
							if len_citations == 0:
								text = get_text_no_cite(cur, post, thread_id, post_count, post_id)
							else:
								text = get_text_cite(cur, post, thread_id, post_count, post_id)
							write_post(cur, thread_id, post_count, post_id, user_url, date_created, text, 0)
							conn.commit()