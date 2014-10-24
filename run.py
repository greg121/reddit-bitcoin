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
                         db=config.get('mysql', 'db_test'),
                         charset="utf8")

    return db

def addToDatabase(db, cur, id, created, date, link, title, body, ups, proc_on, pos, neg, score):
    sql = "INSERT INTO `submissions`(`id`, `timestamp`, `date`, `link`, `title`, `body`, `upvotes`, `processed_on`, `eng_pos`, `eng_neg`, `score`) \
    VALUES ('%s', '%i', '%s', '%s', '%s', '%s', '%i', '%s', '%i', '%i', '%i' )" % \
    (id, created, date, link, title, body, ups, proc_on, pos, neg, score)
    cur.execute(sql)
    db.commit()

def getSubmissions(start):
    r = praw.Reddit(user_agent='get_me_reddit')
    # pro halben Tag (43200sec) eine Abfrage - quelle: http://www.reddit.com/r/redditdev/comments/2j6inf/how_to_download_submissions_of_a_specified_period/
    time_search_param = 'timestamp:' + str(start - 43200) + '..' + str(start)
    print("von", datetime.datetime.utcfromtimestamp(start-43200).strftime('%Y-%m-%d %H:%M:%S'), "bis", datetime.datetime.utcfromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S'))
    submissions = r.search(time_search_param, sort='new', subreddit='bitcoin', syntax='cloudsearch', limit=1000)
    return submissions

def escapeSqlProblems(string):
    result = string.replace("'","''")
    # adressiert das backslash problem
    # möglicherweise noch probleme mit einem String + backslash am Ende
    result = result.replace('\\',' ')
    return result

def main():
    db = connectToDb()
    # you must create a Cursor object. It will let you execute all the queries you need
    cur = db.cursor()
    start = 1413581567
    while 1:
        i = 0
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
            #print(datetime.datetime.now(), "-------------------------")
            date = datetime.datetime.utcfromtimestamp(s.created_utc).strftime('%Y-%m-%d %H:%M:%S')
            #print (str(s.created_utc), date, str(s.id))
            #print (str(s.ups), "-", s.title.encode('ascii', 'ignore'))
            addToDatabase(db,cur,str(s.id),int(s.created_utc),date,s.permalink,escapeSqlProblems(s.title),escapeSqlProblems(s.selftext),s.ups,'',0,0,0)

            # probier 10x reddit zu erreichen
            for attempt in range(10):
                try:
                    # ersetze die more_comments objekte mit den tatsächlichen Kommentaren
                    s.replace_more_comments(limit=None, threshold=0)
                    # problem ist die Struktur von reddit, was hier umgangen wird, indem alles platt geklopft wird
                    flat_comments = praw.helpers.flatten_tree(s.comments)
                except Exception as e:
                    warning(e)
                    time.sleep(4)
                else:
                    break

            for c in flat_comments:
                #nochmal nachlesen: Problem "morecomments"
                #if isinstance(c, praw.objects.Comment):
                date = datetime.datetime.utcfromtimestamp(c.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                #print(str(c.created_utc), date, str(c.id))
                #print ("---", str(c.ups), c.body.encode('ascii', 'ignore'))
                addToDatabase(db,cur,str(c.id),int(c.created_utc),date,c.permalink,'',escapeSqlProblems(c.body),c.ups,'',0,0,0)
        # rewind please! half a day = 43200sec
        start = start - 43201
        print("Anzahl submissions:", i)





    db.close()

if __name__ == '__main__':
    main()
