#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import sys
sys.path.insert(0, '/root/reddit/senti')
import sentistrength
import dbhelper

def updateDatabase(db, cur, pos, neg, score, id):
    sql = "UPDATE `submissions_copy` SET `eng_pos` = '%d', `eng_neg` = '%d', `score` = '%d' WHERE `id` = '%d'" % (pos, neg, score, id)
    cur.execute(sql)
    db.commit()

def escapeSentiProblems(string):
    result = re.sub(r"[^A-Za-z]+", ' ', string)
    return result

def writeLog(string):
    with open("log2", "a") as myfile:
        myfile.write(string + "\n")

def main():
    db = dbhelper.connectToDb("mysql")
    cur = db.cursor()
    start = 1413551441
    cur.execute("SELECT `id`, `title`, `body` FROM `submissions_copy` WHERE `timestamp` BETWEEN '%s' AND '%s'" % (start-10000, start))
    while (cur.rowcount != 0):
        writeLog(str(start) + ": " + str(cur.rowcount))
        result = cur.fetchall()
        for y in range(int(cur.rowcount)):
            id = int(result[y][0])
            string = escapeSentiProblems(result[y][1]) + escapeSentiProblems(result[y][2])
            sentiment = sentistrength.senti(string)
            print sentiment
            pos = int(sentiment[:1])
            neg = int(sentiment[3:4])
            score = pos-neg
            updateDatabase(db, cur, pos, neg, score, id)
        start = start - 10000
        cur.execute("SELECT `id`, `title`, `body` FROM `submissions_copy` WHERE `timestamp` BETWEEN '%s' AND '%s'" % (start-10000, start))



if __name__ == '__main__':
    main()
