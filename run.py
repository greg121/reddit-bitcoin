#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import praw
import datetime

#quelle: http://stackoverflow.com/a/622308/1018288
db = MySQLdb.connect(host="213.165.79.250", # your host, usually localhost
                     user="reddit", # your username
                      passwd="beamer", # your password
                      db="gp_reddit",
                      charset="utf8") # name of the data base

# you must create a Cursor object. It will let you execute all the queries you need
cur = db.cursor()

r = praw.Reddit(user_agent='get_me_reddit')
i = 0
submissions = r.get_subreddit('bitcoin').get_new(limit=10)
for s in submissions:
      i = i + 1
      print "-------------------------"
      date = datetime.datetime.utcfromtimestamp(s.created_utc).strftime('%Y-%m-%d %H:%M:%S')
      print str(s.created_utc) + " " + date + " " + str(s.id)
      print str(s.ups) +" - "+ s.title
      sql = "INSERT INTO `submissions`(`id`, `timestamp`, `date`, `title`, `body`, `upvotes`, `eng_pos`, `eng_neg`, `score`) \
      VALUES ('%s', '%i', '%s', '%s', '%s', '%i', '%i', '%i', '%i' )" % \
      (str(s.id),int(s.created_utc),date,s.title.replace("'","''"),s.selftext.replace("'","''"),s.ups,0,0,0)
      cur.execute(sql)
      db.commit()
      #problem ist die Struktur von reddit, was hier umgangen wird, indem alles platt geklopft wird
      flat_comments = praw.helpers.flatten_tree(s.comments)
      for c in flat_comments:
          #nochmal nachlesen: Problem "morecomments"
          if isinstance(c, praw.objects.Comment):
              date = datetime.datetime.utcfromtimestamp(c.created_utc).strftime('%Y-%m-%d %H:%M:%S')
              print str(c.created_utc) + " " + date + " " + str(c.id)
              print "---" + str(c.ups) + " - " + c.body
              sql = "INSERT INTO `submissions`(`id`, `timestamp`, `date`, `title`, `body`, `upvotes`, `eng_pos`, `eng_neg`, `score`) \
              VALUES ('%s', '%i', '%s', '%s', '%s', '%i', '%i', '%i', '%i' )" % \
              (str(c.id),int(c.created_utc),date,'',c.body.replace("'","''"),c.ups,0,0,0)
              cur.execute(sql)
              db.commit()
print i
db.close()
