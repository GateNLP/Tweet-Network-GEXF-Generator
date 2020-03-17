//Useful discussion on edge decay etc.
//http://mappingonlinepublics.net/2010/10/20/dynamic-networks-in-gephi-from-twapperkeeper-to-gexf/
//https://mappingonlinepublics.net/2010/12/30/visualising-twitter-dynamics-in-gephi-part-1/
//https://mappingonlinepublics.net/2010/12/30/visualising-twitter-dynamics-in-gephi-part-2/

@Grab(group='it.uniroma1.dis.wsngroup.gexf4j', module='gexf4j', version='1.0.0')
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GraphImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.Edge;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeValueList;
import it.uniroma1.dis.wsngroup.gexf4j.core.dynamic.TimeFormat;
import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.dynamic.Spell;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.SpellImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;

import java.util.zip.GZIPInputStream;

@Grab('com.fasterxml.jackson.core:jackson-databind:2.9.8')
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import groovy.cli.OptionField;
import groovy.cli.UnparsedField;

@Grab('joda-time:joda-time:2.9.2')
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Calendar;

CREATED_AT_FORMAT = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withZoneUTC().withLocale(Locale.ENGLISH);

TWEEP_DATE_FORMAT = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm").withZoneUTC().withLocale(Locale.ENGLISH);

@OptionField
Boolean mentions

@OptionField
Boolean retweets

@OptionField
Boolean replies

@OptionField
Boolean trim

@OptionField
Boolean twint

@OptionField
Boolean tweep

@OptionField
Integer edgeWeight

@OptionField
Boolean flow

@UnparsedField List ioOptions

new CliBuilder().parseFromInstance(this, args)

if (!(mentions || retweets || replies)) {
    mentions = true;
    retweets = true;
    replies = true;
}

File outputFile = new File(ioOptions.removeLast());

Gexf gexf = new GexfImpl();
Graph graph = gexf.getGraph();

graph.setDefaultEdgeType(EdgeType.DIRECTED).setMode(Mode.DYNAMIC).setTimeType(TimeFormat.XSDDATETIME);

AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
graph.getAttributeLists().add(attrList);
        
//attLang = attrList.createAttribute("0", AttributeType.STRING, "lang");
attBio = attrList.createAttribute("1", AttributeType.STRING, "bio");
attDisplayName = attrList.createAttribute("2", AttributeType.STRING, "displayName");
//attCity = attrList.createAttribute("3", AttributeType.STRING, "city");
//attCountry = attrList.createAttribute("4", AttributeType.STRING, "country");
//attRegion = attrList.createAttribute("5", AttributeType.STRING, "region");
attFollowers = attrList.createAttribute("6", AttributeType.INTEGER, "followers");
attLocation = attrList.createAttribute("7", AttributeType.STRING, "location");
attFollowing = attrList.createAttribute("8", AttributeType.INTEGER, "following");
attVerified = attrList.createAttribute("9", AttributeType.BOOLEAN, "verified");
attStatusesCount = attrList.createAttribute("10", AttributeType.INTEGER, "statuses_count");

List<File> inputFiles = ioOptions.collect{ new File(it.toString()) };

ObjectMapper objectMapper = new ObjectMapper();

if (tweep && mentions) {
    System.err.println("\n*** mentions are not yet supported for Tweep format files ***\n");
}

while (!inputFiles.isEmpty()) {
    File jsonFile = inputFiles.remove(0);

    if (jsonFile.isDirectory()) {
        System.out.println("recursing into " + jsonFile.getAbsolutePath());
        inputFiles.addAll(jsonFile.listFiles());
        continue;
    }
    
    if (!jsonFile.canRead() || jsonFile.getName().indexOf(".json") == -1) {
        System.out.println("Skipping " + jsonFile.getAbsolutePath());
        continue;
    }

    System.out.println("Reading " + jsonFile.getAbsolutePath());

    InputStream fileIn = new FileInputStream(jsonFile);
    if (jsonFile.getName().endsWith(".gz")) fileIn = new GZIPInputStream(fileIn);
    
    // open the json.gz file for reading
    JsonParser jsonParser = objectMapper.getFactory().createParser(fileIn).enable(Feature.AUTO_CLOSE_SOURCE);

    // If the first token in the stream is the start of an array ("[") then
    // assume the stream as a whole is an array of objects, one per document.
    // To handle this, simply clear the token - The MappingIterator returned by
    // readValues will cope with the rest in either form.
    if(jsonParser.nextToken() == JsonToken.START_ARRAY) {
    jsonParser.clearCurrentToken();
    }

    // Lets get an iterator for working through the separate tweets in the file
    MappingIterator<JsonNode> docIterator = objectMapper.readValues(jsonParser, JsonNode.class);

    while(docIterator.hasNext()) {
        // get the next tweet
        JsonNode tweet = docIterator.next();

	if (twint) convertFromTwint(objectMapper, (ObjectNode)tweet);

	if (tweep) convertFromTweep(objectMapper, (ObjectNode)tweet);

        // classify the tweet so we know what to do with it
        String tweetType = classify(tweet);

        // get the user object from this tweet ready to use it
        JsonNode sourceUser = tweet.at("/user");

        // get the time of the tweet
        DateTime tweetTime = CREATED_AT_FORMAT.parseDateTime(tweet.at("/created_at").asText());

        Node sourceNode = getUserNode(graph, sourceUser);

        if (replies && tweetType.equals("reply")) {
            String inReplyTo = tweet.at("/in_reply_to_screen_name").asText();
            Node targetNode = graph.getNode(inReplyTo.toLowerCase());
            if (targetNode == null) {
                targetNode = graph.createNode(inReplyTo.toLowerCase());
                targetNode.setLabel(inReplyTo);
            }

            Edge edge = getEdge(sourceNode,targetNode);
            incrementWeight(edge);
            addSpell(sourceNode, edge, tweetTime);
        }
        else if (retweets && tweetType.equals("retweet")) {
            Node targetNode = getUserNode(graph, tweet.at("/retweeted_status/user"));

            Edge edge = getEdge(sourceNode,targetNode);
            incrementWeight(edge);
            addSpell(sourceNode, edge, tweetTime);
        }
        else if (retweets && tweetType.equals("quote")) {
            // we treat a quote tweet in the same was as a retweet
            Node targetNode = getUserNode(graph, tweet.at("/quoted_status/user"));

            Edge edge = getEdge(sourceNode,targetNode);
            incrementWeight(edge);
            addSpell(sourceNode, edge, tweetTime);
        }

        if (mentions && !tweetType.equals("retweet")) {
            // if this isn't a retweet then we want to look at the mentions

            JsonNode entities = tweet.at("/extended_tweet/entities/user_mentions");
            if (entities.isMissingNode())
                entities = tweet.at("/entities/user_mentions");

            for (JsonNode mention : entities) {
		String mentioned = mention.at("/screen_name").asText();
                Node targetNode = graph.getNode(mentioned.toLowerCase());
                if (targetNode == null) {
                    targetNode = graph.createNode(mentioned.toLowerCase());
                    targetNode.setLabel(mentioned);
                }

                Edge edge = getEdge(sourceNode,targetNode);
                incrementWeight(edge);
                addSpell(sourceNode, edge, tweetTime);
            }
            
	}
    }
}

System.out.println("\nFull graph consists of " + graph.getNodes().size() + " nodes and "+graph.getAllEdges().size() + " edges");

if (edgeWeight != null) {
    System.out.println("\nRemoving edges with a weight less than or equal to " + edgeWeight+"...");


	for (Edge edge : graph.getAllEdges()) {
	    if (edge.getWeight() <= edgeWeight) {
		Node source = edge.getSource();

		source.getSpells().removeAll(edge.getSpells());
		source.getEdges().remove(edge);
	    }
	}

	Set<Node> toKeep = new HashSet<Node>();
	for (Edge edge : graph.getAllEdges()) {
	    toKeep.add(edge.getSource());
	    toKeep.add(edge.getTarget());
	}

	graph.getNodes().retainAll(toKeep);

	System.out.println("Graph now consists of " + graph.getNodes().size() + " nodes and "+graph.getAllEdges().size() + " edges");
}

if (trim) {
    System.out.println("\nRemoving all but largest connected group...");
    List<Set<Node>> clusters = new ArrayList<Set<Node>>();
    Set<Node> seenNodes = new HashSet<Node>();

    for (Node node : graph.getNodes()) {
        if (seenNodes.contains(node)) continue;

        Set<Node> expanded = new HashSet<Node>();
        expand(node,expanded);

        if (!Collections.disjoint(seenNodes,expanded)) {
            Iterator<Set<Node>> it = clusters.iterator();
            while (it.hasNext()) {
                Set<Node> next = it.next();

                if (!Collections.disjoint(expanded,next)) {
                    it.remove();
                    expanded.addAll(next);
                }
            }
        }

        clusters.add(expanded);

        seenNodes.addAll(expanded);
    }

    int biggest = 0;
    Set<Node> mainCluster = null;
    for (Set<Node> cluster : clusters) {
        if (cluster.size() > biggest) {
            mainCluster = cluster;
            biggest = mainCluster.size();
        }
    }

    graph.getNodes().retainAll(mainCluster);

    for (Node node : mainCluster) {
        Iterator<Edge> eit = node.getEdges().iterator();
        while (eit.hasNext()) {
            Edge edge = eit.next();
            if (!mainCluster.contains(edge.getTarget())) {
                eit.remove();
            }
        }
    }

    System.out.println("Graph now consists of " + graph.getNodes().size() + " nodes and "+graph.getAllEdges().size() + " edges");
}

StaxGraphWriter graphWriter = new StaxGraphWriter();

try {
    Writer out = new FileWriter(outputFile, false);
    graphWriter.writeToStream(gexf, out, "UTF-8");
    System.out.println("\nWriting graph to " + outputFile.getAbsolutePath());
} catch (IOException e) {
    e.printStackTrace();
}

private void incrementWeight(Edge edge) {
    edge.setWeight((float)(edge.getWeight()+1f));
}

private void expand(Node node, Set<Node> seen) {
    if (!seen.add(node)) return;

    for (Edge e : node.getEdges()) {
        expand(e.getTarget(), seen);
    }
}

private void convertFromTwint(ObjectMapper mapper, ObjectNode tweet) {
	ObjectNode userNode = mapper.createObjectNode();
	userNode.put("name", tweet.at("/name").asText());
	userNode.put("screen_name", tweet.at("/username").asText());

	ObjectNode entities = mapper.createObjectNode();
	ArrayNode userMentions = mapper.createArrayNode();

	entities.put("user_mentions", userMentions);
	
	Iterator<JsonNode> it = tweet.at("/mentions").elements();
	
	while (it.hasNext()) {
		ObjectNode mention = mapper.createObjectNode();
		mention.put("screen_name",it.next().asText());
		userMentions.add(mention);
	}

	if (tweet.at("/retweet").asBoolean()) {
		ObjectNode retweet = mapper.createObjectNode();
		tweet.put("retweeted_status",retweet);
		
		ObjectNode retweetUser = mapper.createObjectNode();
		retweetUser.put("screen_name", tweet.at("/user_rt"));
		

		retweet.put("user", userNode);
		tweet.put("user", retweetUser);

		retweet.put("entities", entities);
	}
	else {
		tweet.put("user", userNode);
		tweet.put("entities", entities);
	}

	String tweetText = tweet.at("/tweet").asText();

	if (tweetText.startsWith("@")) {
		//this is a reply now we just need to figure out what the handle is
		String handle = tweetText.split("[^@_0-9a-zA-Z]")[0];

		tweet.put("in_reply_to_screen_name",handle.substring(1));
	}

	tweet.put("text", tweetText);

	tweet.put("created_at",CREATED_AT_FORMAT.print(tweet.at("/created_at").asLong()));

	// mentions is an array of screen names
	// /entities/user_mentions each entry is a User object so set screen_name	
}

private void convertFromTweep(ObjectMapper mapper, ObjectNode tweet) {

/*
{
    "created_at": "26/09/2019 23:59",
    "tweet_id": 1177370000000000000,
    "tweet_text": "RT @SayNoToLabour10: What I think Boris will do:\n\n1) Prorog
ue Parliament for 6 days\n\n2) Have a Queens speech around the 5th-8th of Octobe
r…",
    "screen_name": "purrfect1509",
    "account_creation_date": "Tue Mar 01 16:18:27 +0000 2016",
    "location": "S England, United Kingdom",
    "user_description": "#IStandWithBoris, Pro Brexit 🇬🇧, Crazy Cat lady🐱, Vegan
🌱, I'm always nice until you aren't - FBPE don't bother, Personal Stylist to @Ca
roline_Mucus (parody)",
    "original_tweet_text": "What I think Boris will do:\n\n1) Prorogue Parliamen
t for 6 days\n\n2) Have a Queens speech around the 5th-8th of Octob… https://t.c
o/zSj25IK5is",
    "original_tweet_user_screen_name": "SayNoToLabour10",
    "original_tweet_user_location": "United Kingdom",
    "original_tweet_user_description": "British, Royalist, Conservative, Thatche
rite, Neo-Liberal, Pro-Brexit, Pro-Trump and Pro-Israel.\n°\n°\n°\nWill Follow b
ack Brexiteers\n🇬🇧🇺🇸🇮🇱 #MBGA #MAGA",
    "original_tweet_user_created_at": "26/09/2019 23:04",
    "original_user_account_created_at": "09/06/2018 22:33"
  },
*/
	ObjectNode userNode = mapper.createObjectNode();
	//userNode.put("name", tweet.at("/name").asText());
	userNode.put("screen_name", tweet.at("/screen_name").asText());

	ObjectNode entities = mapper.createObjectNode();
	ArrayNode userMentions = mapper.createArrayNode();

	/*entities.put("user_mentions", userMentions);
	
	Iterator<JsonNode> it = tweet.at("/mentions").elements();
	
	while (it.hasNext()) {
		ObjectNode mention = mapper.createObjectNode();
		mention.put("screen_name",it.next().asText());
		userMentions.add(mention);
	}*/

	if (!tweet.at("/original_tweet_user_screen_name").asText().isEmpty()) {
		ObjectNode retweet = mapper.createObjectNode();
		tweet.put("retweeted_status",retweet);
		
		ObjectNode retweetUser = mapper.createObjectNode();
		retweetUser.put("screen_name", tweet.at("/original_tweet_user_screen_name").asText());
		

		retweet.put("user", userNode);
		tweet.put("user", retweetUser);

		retweet.put("entities", entities);
	}
	else {
		tweet.put("user", userNode);
		tweet.put("entities", entities);
	}

	String tweetText = tweet.at("/tweet_text").asText();

	if (tweetText.startsWith("@")) {
		//this is a reply now we just need to figure out what the handle is
		String handle = tweetText.split("[^@_0-9a-zA-Z]")[0];

		tweet.put("in_reply_to_screen_name",handle.substring(1));
	}

	tweet.put("text", tweetText);

        DateTime tweetTime = TWEEP_DATE_FORMAT.parseDateTime(tweet.at("/created_at").asText());
	tweet.put("created_at",CREATED_AT_FORMAT.print(tweetTime));

	// mentions is an array of screen names
	// /entities/user_mentions each entry is a User object so set screen_name	
}


/**
 * A method to either return an existing directed edge between the source and
 * target nodes, or to create such a node should it not exist.
 **/
private Edge getEdge(Node source, Node target) {
    List<Edge> edges = source.getEdges();

    for (Edge e : edges) {
        if (e.getTarget().getId().equals(target.getId())) {
            return e;
        }
    }

    Edge edge = source.connectTo(target);
    edge.setEdgeType(EdgeType.DIRECTED);
    edge.setWeight(0f);

    return edge;
}

/**
 * A method to either return an existing node representing the user from the
 * graph or to create it if it doesn't already exist. If the node exists but
 * has no attribites (i.e. created for a reply) then the attributes will be
 * added to flesh out the node.
 **/
private Node getUserNode(Graph graph, JsonNode user) {
    String userName = user.at("/screen_name").asText();

    Node userNode = graph.getNode(userName.toLowerCase());

    if (userNode == null) {
        userNode = graph.createNode(userName.toLowerCase());
        userNode.setLabel(userName);
    }

    AttributeValueList attributes = userNode.getAttributeValues();

    if (attributes.isEmpty()) {
        JsonNode node = user.at("/description");
        if (!node.isMissingNode() && !node.isNull())
            attributes.addValue(attBio, node.asText());

        attributes.addValue(attDisplayName, user.at("/screen_name").asText());

        // code examples suggest we should be able to store an actual Integer but it fails
        attributes.addValue(attFollowers, user.at("/followers_count").asText());
        
        node = user.at("/location");
        if (!node.isMissingNode() && !node.isNull())
            attributes.addValue(attLocation, node.asText());

        // code examples suggest we should be able to store an actual Integer but it fails
        attributes.addValue(attFollowing, user.at("/friends_count").asText());

        // code examples suggest we should be able to store an actual Boolean but it fails
        attributes.addValue(attVerified, user.at("/verified").asBoolean().toString());

        // code examples suggest we should be able to store an actual Integer but it fails
        attributes.addValue(attStatusesCount, user.at("/statuses_count").asText());
    }

    return  userNode;
}

/**
 * Returns a classification for the tweet with the possible values being
 * either original, retweet, reply, or quote.
 **/
private String classify(JsonNode tweet) {
    String tweetType = "original";

    JsonNode node = tweet.at("/retweeted_status");
    if (!node.isMissingNode()) {
        // it's a retweet so process that and ignore everything else
        tweetType = "retweet";
    }
    else {
        boolean isReply = (null != tweet.at("/in_reply_to_screen_name").asText(null));
        node  = tweet.at("/quoted_status");
        boolean isComment = !node.isMissingNode();

        if (isReply) {
            tweetType = "reply";
        }
        
        if (isComment) {
            tweetType = "quote";
        }
    }

    return tweetType;
}

/**
 * Add a spell to the node and/or edge based on the provided time stamp. Note
 * that the spell has no length as the start and end time are identical. This
 * is probably wrong and we should look at edges decaying over time rather
 * than simply a series of spikes.
 **/
private void addSpell(Node node, Edge edge, DateTime time) {
    Spell spell = new SpellImpl();

    spell.setStartValue(time.toDate());
    spell.setEndValue(time.toDate());

    if (node != null) node.getSpells().add(spell);
    if (edge != null) edge.getSpells().add(spell);
}
