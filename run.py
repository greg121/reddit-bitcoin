#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
import praw
import datetime
import time
import sys
sys.path.insert(0, '/home/gregor/reddit/senti')
import MySQLdb
import dbhelper
import sentistrength
import re

def addToDatabase(db, cur, created, date, link, title, body, ups, proc_on, pos, neg, score):
    sql = "INSERT INTO `submissions_copy`(`id`, `timestamp`, `date`, `link`, `title`, `body`, `upvotes`, `processed_on`, `eng_pos`, `eng_neg`, `score`) \
    VALUES (NULL, '%s', '%s', '%s', '%s', '%s', '%i', '%s', '%i', '%i', '%i' )" % \
    (created, date, link, title, body, ups, proc_on, pos, neg, score)
    cur.execute(sql)
    db.commit()

def getSubmissions(start):
    r = praw.Reddit(user_agent='get_me_reddit')
    # pro halben Tag (43200sec) eine Abfrage - quelle: http://www.reddit.com/r/redditdev/comments/2j6inf/how_to_download_submissions_of_a_specified_period/
    time_search_param = 'timestamp:' + str(start - 43200) + '..' + str(start)
    writeLog(str(datetime.datetime.now()) + ":    von " + datetime.datetime.utcfromtimestamp(start-43200).strftime('%Y-%m-%d %H:%M:%S') + " bis " + datetime.datetime.utcfromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S'))
    submissions = r.search(time_search_param, sort='new', subreddit='bitcoin', syntax='cloudsearch', limit=1000)
    return submissions

def escapeSqlProblems(string):
    # standardmethode mal testen
    #result = MySQLdb.escape_string(string)

    result = string.replace("'","''")
    result = result.replace('\\',' ')
    return result

def escapeSentiProblems(string):
    result = re.sub(r"[^A-Za-z]+", ' ', string)
    return result

def writeLog(string):
    with open("log", "a") as myfile:
        myfile.write(string + "\n")

def main():
    db = dbhelper.connectToDb('mysql')
    # you must create a Cursor object. It will let you execute all the queries you need
    cur = db.cursor()
    start = 1422374551
    while 1:
        i = 0
        j = 0
        # probier 10x reddit zu erreichen
        for attempt in range(10):
            try:
                submissions = getSubmissions(start)
            except Exception as e:
                writeLog("Fehler bei den Submissions: " + str(e))
                time.sleep(4)
            else:
                break

        for s in submissions:
            sentiment = sentistrength.senti(escapeSentiProblems(s.selftext))
            pos = int(sentiment[:1])
            neg = int(sentiment[3:4])
            i = i + 1
            #print(datetime.datetime.now(), "-------------------------")
            date = datetime.datetime.utcfromtimestamp(s.created_utc).strftime('%Y-%m-%d %H:%M:%S')
            #print (str(s.created_utc), date, str(s.id))
            #print (str(s.ups), "-", s.title.encode('ascii', 'ignore'))
            addToDatabase(db,cur,int(s.created_utc),date,s.permalink,escapeSqlProblems(s.title),escapeSqlProblems(s.selftext),s.ups,'',pos,neg,pos-neg)

            # probier 10x reddit zu erreichen
            for attempt in range(10):
                try:
                    # ersetze die more_comments objekte mit den tatsächlichen Kommentaren
                    s.replace_more_comments(limit=None, threshold=0)
                    # problem ist die Struktur von reddit, was hier umgangen wird, indem alles platt geklopft wird
                    flat_comments = praw.helpers.flatten_tree(s.comments)
                except Exception as e:
                    writeLog("Fehler bei den Kommentaren: " + str(e))
                    time.sleep(4)
                else:
                    break

            for c in flat_comments:
                j = j + 1
                sentiment = sentistrength.senti(escapeSentiProblems(c.body))
                pos = int(sentiment[:1])
                neg = int(sentiment[3:4])
                date = datetime.datetime.utcfromtimestamp(c.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                #print(str(c.created_utc), date, str(c.id))
                #print ("---", str(c.ups), c.body.encode('ascii', 'ignore'))
                addToDatabase(db,cur,int(c.created_utc),date,c.permalink,'',escapeSqlProblems(c.body),c.ups,'',pos,neg,pos-neg)
        # rewind please! half a day = 43200sec
        start = start - 43201
        writeLog("number of submissions: " + str(i))
        writeLog("number of comments: " + str(j))

    db.close()

if __name__ == '__main__':
    main()
