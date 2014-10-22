import praw
import datetime

r = praw.Reddit(user_agent='get_me_reddit')
i = 0
submissions = r.get_subreddit('bitcoin').get_new(limit=10)
for s in submissions:
  i = i + 1
  print str(s.ups) +" - "+ s.title
  flat_comments = praw.helpers.flatten_tree(s.comments)
  for c in flat_comments:
    if isinstance(c, praw.objects.Comment):
      print str(c.created_utc) + " " + datetime.datetime.utcfromtimestamp(c.created_utc).strftime('%Y-%m-%d %H:%M:%S')
      print "---" + str(c.ups) + " - " + c.body

print i
