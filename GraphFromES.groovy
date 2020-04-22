@Grab('com.fasterxml.jackson.core:jackson-databind:2.9.8')
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

class GraphFromES {
    ObjectMapper objectMapper = new ObjectMapper();
    GexfGenerator gexfGen
    String esURL
    def esQuery

    public GraphFromES(String esParamsAsJSON) {
        def esParams = new JsonSlurper().parseText(esParamsAsJSON)
        esURL = esParams.esURL
        esQuery = esParams.esQuery

        gexfGen = new GexfGenerator()
        gexfGen.mentions = esParams.mentions
        gexfGen.retweets = esParams.retweets
        gexfGen.replies = esParams.replies
        gexfGen.trim = esParams.trim
        gexfGen.twint = esParams.twint
        gexfGen.tweep = esParams.tweep
        gexfGen.edgeWeight = esParams.edgeWeight
        gexfGen.flow = esParams.flow
        gexfGen.outputFile = new File(esParams.outputFile)
    }


    // Set header to json
    static void main(String[] args) {
        String esParamsAsJSON = '''
            {
                "esURL":"https://weverify-tsna-es.gate.ac.uk/twinttweets/_search",
                "mentions":true,
                "retweets":true,
                "replies":true,
                "trim":false,
                "twint":true,
                "tweep":false,
                "flow":false,
                "outputFile":"./test/es1.gexf",
                "esQuery":{"aggs":{"retweets":{"sum" :{"field":"nretweets"}},"likes": {"sum":{"field":"nlikes"}}},"size":10000,"_source":{"excludes":[]},"stored_fields":["*"],"script_fields":{},"query":{"bool":{"must":[{"query_string":{"query":"NOT _exists_:likes NOT _exists_:retweets NOT _exists_:replies","analyze_wildcard":true,"time_zone":"Europe/Paris"}},{"match_phrase": {"tweet": {"query":"fake"}}},{"match_phrase": {"username": {"query":"realdonaldtrump"}}},{"range":{"date":{"format":"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis","gte":"2020-01-01 00:00:00","lte":"2020-04-20 00:00:00"}}}],"filter":[],"should":[],"must_not":[]}},"sort":[{"date":{"order":"asc"}}]}
            }
        '''
        GraphFromES gFromEs = new GraphFromES(esParamsAsJSON.toString())

        generateGraph(gFromEs)
    }

    private static void generateGraph(GraphFromES gFromEs) {
        URL url = new URL(gFromEs.esURL);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json; utf-8");
        con.setRequestProperty("Accept", "application/json");
        con.setDoOutput(true);
        StringBuilder response = new StringBuilder();
        //TODO either enable try-with, or close the streams
        try {
            OutputStream os = con.getOutputStream()
            byte[] input = JsonOutput.toJson(gFromEs.esQuery).getBytes("utf-8");
            os.write(input, 0, input.length);

            BufferedReader br = new BufferedReader(
                    new InputStreamReader(con.getInputStream(), "utf-8"))
            String responseLine
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
        }
        catch (Exception ex) {
            print ex.message
        }
        def hits = new JsonSlurper().parseText(response.toString())["hits"]
        //add tweets to the graph
        for (hit in hits["hits"]) {
            println JsonOutput.toJson(hit._source)
            JsonNode jsNode = gFromEs.objectMapper.readTree(JsonOutput.toJson(hit._source))
            gFromEs.gexfGen.addTweet(jsNode)
        }
        gFromEs.gexfGen.writeToFile()
    }
}
