# Tweet Network GEXF Generator

To generate a GEXF network graph use the following command

```
groovy export.groovy [options] <input files> <output file>
```

Note that you need Groovy 2.5 or above for the script to work correctly

where options are

```
--retweets: includes retweets within the graph
 --replies: includes replies within the graph
--mentions: includes mentions within the graph
   --twint: assumes the data is in Twint format not native Twitter style
   --tweep: assumes the data is in Tweep format not native Twitter style
```

if no options are provided then rather than producing an empty graph, it is
assumed that the user wants retweets, replies, and mentions.

Any number of input files or dirctories can be provided to the script and these
will be recursed to find all *.gz files which it assumes contains Tweets in
JSON format, one tweet per line. This is the format in which Tweets are made
available for download from GATE Cloud.

The final arguent should be the name of the GEXF file to write the graph to.

## Known Issues
- When processing retweets in Twint data it's unclear if the timestamp is for
  the retweet or the original tweet
- Replies in both Twint and Tweep data are horrid as they are just the tweets
  that start @
- Unclear in Twint if the mentions includes the person being replied to or not
  (we don't want them to be as that would duplicate the edge)
- Tweep format doesn't pull out the mentions and we've not yet added support
  for pulling them out of the tweet text
