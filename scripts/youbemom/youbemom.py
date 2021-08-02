#!/usr/bin/env python3
# coding: utf-8

# Contains functions for scraping Youbemom forum posts


import re
import sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from dateutil.parser import parse
from scraping import *

# ## Functions
# For accessing the database

def set_up_db(conn):
    """ if the database exists, drop it and create a
        SQLite database for the results
    :param conn: database connection
    :return: nothing
    """
    cur = conn.cursor()
    cur.executescript('''
        CREATE TABLE IF NOT EXISTS threads (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            family_id INTEGER,
            url TEXT,
            subforum TEXT,
            dne INTEGER
        );
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            family_id INTEGER,
            message_id TEXT,
            parent_id INTEGER,
            date_recorded TEXT,
            date_created TEXT,
            title TEXT,
            body TEXT,
            subforum TEXT,
            deleted INTEGER
        );
    ''')

def write_to_threads(conn, family_id, url, subforum, dne):
    """ inserts the parsed data into the threads table
    :param parsed: a tuple of the parsed data
    :return: nothing
    """
    sql = ''' INSERT INTO threads(family_id, url, subforum, dne)
    VALUES(?,?,?,?) '''
    parsed = (family_id, url, subforum, dne)
    cur = conn.cursor()
    cur.execute(sql, parsed)
    conn.commit()

def write_to_posts(parsed, conn):
    """ inserts the parsed data into the posts table
    :param parsed: a tuple of the parsed data
    :return: nothing
    """
    sql = ''' INSERT INTO posts(family_id,message_id,parent_id,date_recorded,date_created,title,body,subforum,deleted)
    VALUES(?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, parsed)
    conn.commit()

# For parsing post text

def post_dne(soup):
    title_html = soup.find("h1")
    title = title_html.get_text()
    if "Sorry, there was a problem!" == title:
        p_html = soup.find("p")
        p = p_html.get_text()
        if "The page you were looking for could not be found." in p:
            return 1
    return 0

def fix_ago(date_created, date_recorded):
    """ if the post date created includes a relative
        instead of absolute time (ago vs m-d-y), fix
        and replace the time
    :param date_created: the date the post was created
    :param date_recorded: date recorded by the scraper
    :return dc: date_created in datetime
    """
    if "hr" in date_created:
        if "min" in date_created:
            l = re.findall("[0-9]+", date_created)
            dc = datetime.strptime(date_recorded, "%m-%d-%Y %H:%M:%S") - timedelta(hours=int(l[0]), minutes=int(l[1]))
        else:
            l = re.findall("[0-9]+", date_created)[0]
            dc = datetime.strptime(date_recorded, "%m-%d-%Y %H:%M:%S") - timedelta(hours=int(l))        
    else:
        l = re.findall("[0-9]+", date_created)[0]
        dc = datetime.strptime(date_recorded, "%m-%d-%Y %H:%M:%S") - timedelta(minutes=int(l))
    return dc

def fix_date(date_created):
    """ removes extra text from date
    :param date_created: string of date
    :return dc: stripped string of date
    """
    dc = date_created.replace('posted ','')
    dc = dc.replace(' in Tween/Teen','')
    dc = dc.replace(' in Elementary','')
    dc = dc.replace(' in Preschool','')
    dc = dc.replace(' in Toddler','')
    dc = dc.replace(' in Newborn','')
    dc = dc.replace(' in Special Needs','')
    dc = dc.replace(' in Expecting','')
    dc = dc.replace(' in TTC','')
    dc = dc.replace(' in Single Parents','')
    dc = dc.replace(' in Weight Watchers','')
    dc = dc.replace(' in YBM Feedback','')
    dc = dc.replace(' in Boston','')
    dc = dc.replace(' in Chicago','')
    dc = dc.replace(' in Los Angeles','')
    dc = dc.replace(' in New York City','')
    dc = dc.replace(' in NYC Schools','')
    return dc

def get_subforum(soup):
    action = soup.find('form', {'id' : 'search'})
    if action:
        subforum = action['action']
        subforum = subforum.replace("/forum/","")
        return subforum
    return False

def clean_text(text):
    """ clean the text of extra spaces, new lines,
        ellipses, and (more) text
    :param text: input text
    :return text: cleaned text
    """
    text = text.strip()
    text = re.sub("\(more\)", "", text)
    text = text.strip()
    text = re.sub("\s+", " ", text)
    text = re.sub("\n", "", text)
    text = re.sub("\.{3}", "", text)
    return text

def parse_post_parent(soup, conn, family_id, date_recorded, subforum):
    """ parse the list items into a format that can be
        inserted into the database (top post in thread)
    :param soup: input soup of parent post
    :param conn: connection to db
    :param date_recorded: date scraping the data
    """
    title_html = soup.find("h1")
    title = title_html.get_text()
    title = clean_text(title)
    if title_html.has_attr('class') and "removed" in title_html['class']:
        deleted = 1
    else:
        deleted = 0
    message_id = title_html["id"]
    body_html = soup.find('div', {'class' : 'message', 'id' : "p" + message_id})
    if body_html:
        body = body_html.get_text()
        body = body.replace('log in or sign up to post a comment', '')
        body = clean_text(body)
    else:
        body = ""
    date_created = soup.find('div', {'class' : 'date'}).get_text()
    # if doesn't contain "ago", change time with strptime
    if "ago" in date_created:
        date_created = fix_ago(date_created, date_recorded)
    else:
        date_created = fix_date(date_created)
        date_created = parse(date_created)
    parsed = (family_id,message_id,"",date_recorded,date_created,title,body,subforum,deleted)
    write_to_posts(parsed, conn)
    return message_id

def parse_post_child(soup, conn, family_id, parent_id, date_recorded, subforum):
    """ parse the list items into a format that can be
        inserted into the database (child replys)
    :param soup: input soup of child post
    :param conn: connection to db
    :param family_id: id of the family thread
    :param parent_id: id of the parent post to this child
    :param date_recorded: date scraping the data
    :NOTE: unlike top post, must re.compile class because
           it might be class='noskimwords reply removed'
    """
    title_html = soup.find('span', {'class' : re.compile('noskimwords reply')})
    title = title_html.get_text()
    title = clean_text(title)
    if title_html.has_attr('class') and "removed" in title_html['class']:
        deleted = 1
    else:
        deleted = 0
    message_id = title_html["id"]
    body_html = soup.find('div', {'class' : 'message', 'id' : "p" + message_id})
    if body_html:
        body = body_html.get_text()
        body = body.replace('log in or sign up to post a comment', '')
        body = clean_text(body)
    else:
        body = ""
    date_created = soup.find('span', {'class' : 'meta date'}).get_text()
    # if doesn't contain "ago", change time with strptime
    if "ago" in date_created:
        date_created = fix_ago(date_created, date_recorded)
    else:
        date_created = fix_date(date_created)
        date_created = parse(date_created)
    parsed = (family_id,message_id,parent_id,date_recorded,date_created,title,body,subforum,deleted)
    write_to_posts(parsed, conn)
    return message_id

def search_children(children, conn, family_id, parent_id, date_recorded, subforum):
    for child in children:
        message_id = parse_post_child(child, conn, family_id, parent_id, date_recorded, subforum)
        replies = child.find('ul')
        if replies:
            grandchildren = replies.find_all("li", recursive=False)
            search_children(grandchildren, conn, family_id, message_id, date_recorded, subforum)


# For looping through the forum

def loop_link_threads(conn, path_db, earliest_link, last_link):
    forum_url = "https://www.youbemom.com/forum/permalink/"
    sql = """ SELECT MAX(family_id) FROM threads """
    cur = conn.cursor()
    cur.execute(sql)
    max_id = cur.fetchone()[0]
    if max_id:
        next_id = int(max_id) + 1
    else:
        next_id = 1
    for post_num in range(earliest_link + next_id - 1, last_link):
        next_url = forum_url + str(post_num)
        soup = get_soup(next_url)
        if soup:
            url = "/forum/permalink/" + str(post_num)
            dne = post_dne(soup)
            if dne == 1:
                write_to_threads(conn, next_id, url, "none", dne)
            else:
                subforum = get_subforum(soup)
                if subforum:
                    write_to_threads(conn, next_id, url, subforum, dne)
                    parse_link(conn, path_db, subforum, next_id, url)
        next_id += 1
    return

def parse_link(conn, path_db, subforum, family_id, url):
    date_recorded = datetime.now().strftime("%m-%d-%Y %H:%M:%S")
    if 'https://www.youbemom.com' not in url or 'http://www.youbemom.com' not in url:
        url = 'https://www.youbemom.com' + url
    soup = get_soup(url)
    if soup:
        message_id = parse_post_parent(soup, conn, family_id, date_recorded, subforum)
        replies = soup.find('ul', {'id' : 'reply-list'})
        if replies:
            children = replies.find_all('li', recursive=False)
            search_children(children, conn, family_id, message_id, date_recorded, subforum)
    return

def loop_list_links(conn, path_db, missing_ids, min_permalink):
    forum_url = "https://www.youbemom.com/forum/permalink/"
    for missing_id in missing_ids:
        permalink = min_permalink + missing_id - 1
        next_url = forum_url + str(permalink)
        print("getting {}".format(next_url))
        soup = get_soup(next_url)
        if soup:
            url = "/forum/permalink/" + str(permalink)
            dne = post_dne(soup)
            if dne == 1:
                write_to_threads(conn, missing_id, url, "none", dne)
            else:
                subforum = get_subforum(soup)
                if subforum:
                    write_to_threads(conn, missing_id, url, subforum, dne)
                    parse_link(conn, path_db, subforum, missing_id, url)
    return
