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

def getSubmissions(start):
    r = praw.Reddit(user_agent='get_me_reddit')
    # pro Tag eine Abfrage - quelle: http://www.reddit.com/r/redditdev/comments/2j6inf/how_to_download_submissions_of_a_specified_period/
    time_search_param = 'timestamp:' + str(start - 86600) + '..' + str(start)
    print("von", datetime.datetime.utcfromtimestamp(start-86600).strftime('%Y-%m-%d %H:%M:%S'), "bis", datetime.datetime.utcfromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S'))
    submissions = r.search(time_search_param, sort='new', subreddit='bitcoin', syntax='cloudsearch')
    return submissions

def main():
    db = connectToDb()
    # you must create a Cursor object. It will let you execute all the queries you need
    cur = db.cursor()
    i = 0
    start = 1413581567
    while 1:
        # probier 10x reddit zu erreichen
        for attempt in range(10):
            try:
                submissions = getSubmissions(start)
            except Exception as e:
                warning(e)
                time.sleep(4)
            else:
                break

        for s in submissions:
            i = i + 1
            print(datetime.datetime.now(), "-------------------------")
            date = datetime.datetime.utcfromtimestamp(s.created_utc).strftime('%Y-%m-%d %H:%M:%S')
            print (str(s.created_utc), date, str(s.id))
            print (str(s.ups), "-", s.title.encode('ascii', 'ignore'))
            sql = "INSERT INTO `submissions`(`id`, `timestamp`, `date`, `title`, `body`, `upvotes`, `processed_on`, `eng_pos`, `eng_neg`, `score`) \
            VALUES ('%s', '%i', '%s', '%s', '%s', '%i', '%s', '%i', '%i', '%i' )" % \
            (str(s.id),int(s.created_utc),date,s.title.replace("'","''"),s.selftext.replace("'","''"),s.ups,'',0,0,0)
            cur.execute(sql)
            db.commit()
            # probier 10x reddit zu erreichen
            for attempt in range(10):
                try:
                    # problem ist die Struktur von reddit, was hier umgangen wird, indem alles platt geklopft wird
                    flat_comments = praw.helpers.flatten_tree(s.comments)
                except Exception as e:
                    warning(e)
                    time.sleep(4)
                else:
                    break

            for c in flat_comments:
                #nochmal nachlesen: Problem "morecomments"
                if isinstance(c, praw.objects.Comment):
                    date = datetime.datetime.utcfromtimestamp(c.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                    print(str(c.created_utc), date, str(c.id))
                    print ("---", str(c.ups), c.body.encode('ascii', 'ignore'))
                    sql = "INSERT INTO `submissions`(`id`, `timestamp`, `date`, `title`, `body`, `upvotes`, `processed_on`, `eng_pos`, `eng_neg`, `score`) \
                    VALUES ('%s', '%i', '%s', '%s', '%s', '%i', '%s', '%i', '%i', '%i' )" % \
                    (str(c.id),int(c.created_utc),date,'',c.body.replace("'","''"),c.ups,'',0,0,0)
                    cur.execute(sql)
                    db.commit()
        # einen Tag zurück, bitte!
        start = start - 86601
        print("Anzahl submissions:", i)





    db.close()

if __name__ == '__main__':
    main()
