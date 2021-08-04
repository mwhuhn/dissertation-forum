earliest = 6987340 # start 2014/01/01: 6987340
latest = 11043529 # first post in 2021/01/01: 11043529
groups = 10
size = round((latest-earliest)/groups)

text = """
## Imports

from youbemom import loop_link_threads, create_connection, set_up_db
from pathlib import Path

## File Locations

p = Path.cwd()
path_parent = p.parents[0]
path_db = path_parent / "database" / "youbemomTables-{0}.db"
path_db = str(path_db)

## Connect to the database and create the tables.

conn = create_connection(path_db)
set_up_db(conn)

## Scrape threads

earliest_link = {1}
last_link = {2}
loop_link_threads(conn, path_db, earliest_link, last_link)

conn.close()
"""

for g in range(groups):
    first = earliest + (size * g)
    if g == groups - 1:
        last = latest
    else:
        last = earliest + (size * (g + 1))
    if g + 1 < 10:
        num = "0{}".format(g + 1)
    else:
        num = "{}".format(g + 1)
    with open("group_{}.py".format(num), 'w') as w:
        w.write(text.format(num, first, last))
