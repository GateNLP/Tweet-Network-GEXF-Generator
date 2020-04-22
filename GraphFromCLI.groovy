@Grab('com.fasterxml.jackson.core:jackson-databind:2.9.8')

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.cli.OptionField
import groovy.cli.UnparsedField
import java.util.zip.GZIPInputStream

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

Boolean isCLI = true

new CliBuilder().parseFromInstance(this, args)
if (!(mentions || retweets || replies)) {
    mentions = true;
    retweets = true;
    replies = true;
}
File outputFile = new File(ioOptions.removeLast());

List<File> inputFiles = ioOptions.collect { new File(it.toString()) }


GexfGenerator gexfGen = new GexfGenerator()
gexfGen.mentions=mentions
gexfGen.retweets=retweets
gexfGen.replies=replies
gexfGen.trim=trim
gexfGen.twint=twint
gexfGen.tweep=tweep
gexfGen.edgeWeight=edgeWeight
gexfGen.flow=flow
gexfGen.outputFile = outputFile

ObjectMapper objectMapper = new ObjectMapper();
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
    JsonParser jsonParser = objectMapper.getFactory().createParser(fileIn).enable(JsonParser.Feature.AUTO_CLOSE_SOURCE);

    // If the first token in the stream is the start of an array ("[") then
    // assume the stream as a whole is an array of objects, one per document.
    // To handle this, simply clear the token - The MappingIterator returned by
    // readValues will cope with the rest in either form.
    if (jsonParser.nextToken() == JsonToken.START_ARRAY) {
        jsonParser.clearCurrentToken();
    }

    // Lets get an iterator for working through the separate tweets in the file
    MappingIterator<JsonNode> docIterator = objectMapper.readValues(jsonParser, JsonNode.class);

    while (docIterator.hasNext()) {
        // get the next tweet
        JsonNode tweet = docIterator.next();
        gexfGen.addTweet(tweet)
    }
}
//apply edge weights and trim option, and output the graph
gexfGen.writeToFile()