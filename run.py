#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
import MySQLdb
import praw
import datetime
import ConfigParser
import time
import sys

def warning(*objs):
    print(datetime.datetime.now(), "WARNING: ", *objs, file=sys.stderr)

def connectToDb():
    # config einlesen
    config = ConfigParser.RawConfigParser()
    config.read('config.ini')

    # quelle: http://stackoverflow.com/a/622308/1018288
    db = MySQLdb.connect(host=config.get('mysql', 'host'),
                         user=config.get('mysql', 'user'),
                         passwd=config.get('mysql', 'passwd'),
                         db=config.get('mysql', 'db'),
                         charset="utf8")

    return db

def getSubmissions():
    r = praw.Reddit(user_agent='get_me_reddit')
    submissions = r.get_subreddit('bitcoin').get_new(limit=10)
    return submissions

def main():
    db = connectToDb()
    # you must create a Cursor object. It will let you execute all the queries you need
    cur = db.cursor()
    submissions = getSubmissions()
    i = 0
    for s in submissions:
          i = i + 1
          print(datetime.datetime.now(), "-------------------------")
          date = datetime.datetime.utcfromtimestamp(s.created_utc).strftime('%Y-%m-%d %H:%M:%S')
          print (str(s.created_utc), date, str(s.id))
          print (str(s.ups), "-", s.title.encode('ascii', 'ignore'))
          sql = "INSERT INTO `submissions`(`id`, `timestamp`, `date`, `title`, `body`, `upvotes`, `eng_pos`, `eng_neg`, `score`) \
          VALUES ('%s', '%i', '%s', '%s', '%s', '%i', '%i', '%i', '%i' )" % \
          (str(s.id),int(s.created_utc),date,s.title.replace("'","''"),s.selftext.replace("'","''"),s.ups,0,0,0)
          cur.execute(sql)
          db.commit()
          # problem ist die Struktur von reddit, was hier umgangen wird, indem alles platt geklopft wird
          for attempt in range(10):
              try:
                  flat_comments = praw.helpers.flatten_tree(s.comments)
              except Exception as e:
                  warning(e)

          for c in flat_comments:
              #nochmal nachlesen: Problem "morecomments"
              if isinstance(c, praw.objects.Comment):
                  date = datetime.datetime.utcfromtimestamp(c.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                  print(str(c.created_utc), date, str(c.id))
                  print ("---", str(c.ups), c.body.encode('ascii', 'ignore'))
                  sql = "INSERT INTO `submissions`(`id`, `timestamp`, `date`, `title`, `body`, `upvotes`, `eng_pos`, `eng_neg`, `score`) \
                  VALUES ('%s', '%i', '%s', '%s', '%s', '%i', '%i', '%i', '%i' )" % \
                  (str(c.id),int(c.created_utc),date,'',c.body.replace("'","''"),c.ups,0,0,0)
                  cur.execute(sql)
                  db.commit()





    print(i)
    db.close()

if __name__ == '__main__':
    main()
