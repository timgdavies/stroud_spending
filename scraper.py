# This is a template for a Python scraper on morph.io (https://morph.io)
# including some code snippets below that you should find helpful

import scraperwiki
from bs4 import BeautifulSoup
from contextlib import closing
import requests
import csv
from datetime import datetime


#
# # Find something on the page using css selectors
# root = lxml.html.fromstring(html)
# root.cssselect("div[align='left']")
#
# # Write out to the sqlite database using scraperwiki library
# scraperwiki.sqlite.save(unique_keys=['name'], data={"name": "susan", "occupation": "software developer"})
#
# # An arbitrary query against the database
# scraperwiki.sql.select("* from data where 'name'='peter'")

# You don't have to do things with the scraperwiki and lxml libraries.
# You can use whatever libraries you want: https://morph.io/documentation/python
# All that matters is that your final data is written to an SQLite database
# called "data.sqlite" in the current working directory which has at least a table
# called "data".

html = requests.get("https://www.stroud.gov.uk/council/opendata/council-expenditure-over-500").text

soup = BeautifulSoup(html, 'html.parser')

for a in soup.find_all('a'):
    if '.csv' in a.get('href',''):
        url = "https://www.stroud.gov.uk" + a.get('href')
        print url
        try:
            with closing(requests.get(url, stream=True)) as r:
                f = (line for line in r.iter_lines())
                headers = ["Supplier Name","Company No","Responsible Unit","Expenses Code","Date Paid","Transaction Number","Amount","Service Area Categorisation"]

                reader = csv.DictReader(f, delimiter=',', quotechar='"',fieldnames=headers,restkey="remove")
                for row in reader:
                    row['hash'] = hash(frozenset(row))
                    try: # Update the date column to give us a real date we can work with
                        row['Date'] = datetime.strptime(row['Date Paid'],"%d-%b-%y")
                    except Exception as e:
                        row['Date'] = ""
                    try: # remove any blanks
                        del(row[''])
                    except Exception:
                        pass
                    try: # remove any blanks
                        del(row['remove'])
                    except Exception:
                        pass
                    print row
                    try:
                        scraperwiki.sqlite.save(unique_keys=['hash'],data=row,table_name='data')
                    except Exception as e:
                        print(e)
                        print("Failed to save row")

        except Exception as e:
           print(e)
           print "Failed to convert "+ url
