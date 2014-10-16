import praw
import datetime
from pprint import pprint

r = praw.Reddit('whatevs')
submission = r.get_submission(submission_id = "2gn66w")
flat_comments = praw.helpers.flatten_tree(submission.comments)
for c in flat_comments:
  if isinstance(c, praw.objects.Comment):
    print str(c.created_utc) + " " + datetime.datetime.fromtimestamp(c.created_utc).strftime('%Y-%m-%d %H:%M:%S')
    print c.body
    print c.ups
