import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Rocko on 2016/11/3.
 */
public class DynamoDBSampleTest {

    private AmazonDynamoDBClient client;

    @Before
    public void setUp(){
        client = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider());
        client.setRegion(RegionUtils.getRegion("us-east-1"));
        //client.setEndpoint("http://127.0.0.1:33061");
    }

    /**
     * 建立 Table
     */
    @Test
    public void dynamoCreateTable(){
        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "Movies";

        try {

            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(
                            new KeySchemaElement("year", KeyType.HASH),  //Partition key
                            new KeySchemaElement("title", KeyType.RANGE)), //Sort key
                    Arrays.asList(
                            new AttributeDefinition("year", ScalarAttributeType.N),
                            new AttributeDefinition("title", ScalarAttributeType.S)),
                    new ProvisionedThroughput(1L, 1L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        } catch (Exception e) {

            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
    }

    /**
     * 由外部檔案載入Data
     * @throws IOException
     */
    @Test
    public void createDynamoDbItem() throws IOException {

        ClassLoader classLoader = getClass().getClassLoader();
        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "Movies";

        Table table = dynamoDB.getTable(tableName);

        JsonParser parser = new JsonFactory()
                .createParser(new File(classLoader.getResource("moviedata.json").getFile()));

        JsonNode rootNode = new ObjectMapper().readTree(parser);
        Iterator<JsonNode> iter = rootNode.iterator();

        ObjectNode currentNode;

        while (iter.hasNext()) {
            currentNode = (ObjectNode) iter.next();

            int year = currentNode.path("year").asInt();
            String title = currentNode.path("title").asText();

            try {
                table.putItem(new Item()
                        .withPrimaryKey("year", year, "title", title)
                        .withJSON("info", currentNode.path("info").toString()));
                System.out.println("PutItem succeeded: " + year + " " + title);

            } catch (Exception e) {
                System.err.println("Unable to add movie: " + year + " " + title);
                System.err.println(e.getMessage());
                break;
            }
        }
        parser.close();
    }

    /**
     * 加入 Item
     * @throws IOException
     */
    @Test
    public void createDynamoDbOpsItem() throws IOException {

        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "Movies";

        Table table = dynamoDB.getTable(tableName);

        int year = 2015;
        String title = "The Big New Movie";

        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("plot",  "Nothing happens at all.");
        infoMap.put("rating",  0);

        try {
            System.out.println("Adding a new item...");
            PutItemOutcome outcome = table.putItem(new Item()
                    .withPrimaryKey("year", year, "title", title)
                    .withMap("info", infoMap));

            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

        } catch (Exception e) {
            System.err.println("Unable to add item: " + year + " " + title);
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void createDynamoDbOpsdelItem() throws IOException {

        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "domain_gid";

        Table table = dynamoDB.getTable(tableName);

        String domain = "morning";
        String visit_time = "22222222";

        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("plot",  "Nothing happens at all.");
        infoMap.put("rating",  0);

        try {
            System.out.println("Adding a new item...");
            PutItemOutcome outcome = table.putItem(new Item()
                    .withPrimaryKey("domain", domain, "visit_time", visit_time)
                    .withMap("info", infoMap));

            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

        } catch (Exception e) {
            System.err.println("Unable to add item: " + domain + " " + visit_time);
            System.err.println(e.getMessage());
        }
    }


    /**
     * 找到 Item
     * @throws IOException
     */
    @Test
    public void getDynamoDbOpsItem() throws IOException {

        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "Movies";

        Table table = dynamoDB.getTable(tableName);

        int year = 2015;
        String title = "The Big New Movie";

        GetItemSpec spec = new GetItemSpec()
                .withPrimaryKey("year", year, "title", title);

        try {
            System.out.println("Attempting to read the item...");
            Item outcome = table.getItem(spec);
            System.out.println("GetItem succeeded: " + outcome);

        } catch (Exception e) {
            System.err.println("Unable to read item: " + year + " " + title);
            System.err.println(e.getMessage());
        }
    }

    /**
     * 找到 Item
     * @throws IOException
     */
    @Test
    public void getDynamoDbOpsDmpItem() throws IOException {

        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "domain_gid";

        Table table = dynamoDB.getTable(tableName);

        HashMap<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#domain", "domain");

        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":domain", "etungo.com.tw");

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#domain = :domain")
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;

        try {

            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getString("domain") + ": "
                        + item.getString("fid"));
            }

        } catch (Exception e) {
            System.err.println("Unable to query movies from 1985");
            System.err.println(e.getMessage());
        }
    }


    /**
     * 找到 Item
     * @throws IOException
     */
    @Test
    public void getDynamoDbOpsDmpScanItem() throws IOException {

        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "domain_gid";

        Table table = dynamoDB.getTable(tableName);


        Map<String, AttributeValue> expressionAttributeValues = new HashMap();
        expressionAttributeValues.put(":start_time", new AttributeValue().withS("1478246130"));
        expressionAttributeValues.put(":end_time", new AttributeValue().withS("1478246139"));

        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableName)
                .withFilterExpression("visit_time between :start_time and :end_time")
                .withProjectionExpression("visit_time")
                //.withAttributesToGet("gid")
                .withExpressionAttributeValues(expressionAttributeValues);

        try {

                System.out.println("Start Scan...");
                ScanResult scanResult = client.scan(scanRequest);
                System.out.println("End Scan, get items " + String.valueOf(scanResult.getItems().size()));

                Map<String, Object> insertData = new HashMap();

//            scanResult.getItems().forEach(item -> {
//
//                log.info("Items: {}", item);
//                String visit_time = item.get("visit_time").getS();
//                String gid = item.get("gid").getS();
//                log.info(" Visit time: {}, Gid:{}",  visit_time, gid);
//                log.info("Domain: {}, Visit time: {}, Gid:{}", domain, visit_time, gid);
//                insertData.put("domain", "domain");
//                insertData.put("visit_time", visit_time);
//                insertData.put("gid", gid);
//                jdbcConnect.insertGtmData(insertData);
//            });
        } catch (Exception e) {
            System.out.println("Query DynamoDB Error: " + e.getMessage());
        }

//        ScanSpec scanSpec = new ScanSpec()
//                .withProjectionExpression("#visitTime, fid, gid")
//                .withFilterExpression("#visitTime between :start_time and :end_time")
//                .withNameMap(new NameMap().with("#visitTime",  "visit_time"))
//                .withValueMap(new ValueMap().withNumber(":start_time", 1478242380).withNumber(":end_time", 1478242389));
//
//        try {
//            ItemCollection<ScanOutcome> items = table.scan(scanSpec);
//
//            Iterator<Item> iter = items.iterator();
//            while (iter.hasNext()) {
//                Item item = iter.next();
//                System.out.println(item.toString());
//            }
//
//        } catch (Exception e) {
//            System.err.println("Unable to scan the table:");
//            System.err.println(e.getMessage());
//        }

    }


    /**
     * 更新 Item
     * @throws IOException
     */
    @Test
    public void updateDynamoDbOpsItem() throws IOException {

        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "Movies";

        Table table = dynamoDB.getTable(tableName);

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey("year", year, "title", title)
                .withUpdateExpression("set info.rating = :r, info.plot=:p, info.actors=:a")
                .withValueMap(new ValueMap()
                        .withNumber(":r", 5.5)
                        .withString(":p", "Everything happens all at once.")
                        .withList(":a", Arrays.asList("Larry","Moe","Curly")))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            System.out.println("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Unable to update item: " + year + " " + title);
            System.err.println(e.getMessage());
        }
    }


    /**
     * Get Items
     */
    @Test
    public void getDynamoDBItems(){
        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "Movies";

        Table table = dynamoDB.getTable(tableName);
        HashMap<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#yr", "year");

        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":yyyy", 1985);

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#yr = :yyyy")
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;

        try {
            System.out.println("Movies from 1985");
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getNumber("year") + ": "
                        + item.getString("title"));
            }

        } catch (Exception e) {
            System.err.println("Unable to query movies from 1985");
            System.err.println(e.getMessage());
        }

        valueMap.put(":yyyy", 1992);
        valueMap.put(":letter1", "A");
        valueMap.put(":letter2", "L");

        querySpec
                .withProjectionExpression(
                        "#yr, title, info.genres, info.actors[0]")
                .withKeyConditionExpression(
                        "#yr = :yyyy and title between :letter1 and :letter2")
                .withNameMap(nameMap).withValueMap(valueMap);

        try {
            System.out
                    .println("Movies from 1992 - titles A-L, with genres and lead actor");
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getNumber("year") + ": "
                        + item.getString("title") + " " + item.getMap("info"));
            }

        } catch (Exception e) {
            System.err.println("Unable to query movies from 1992:");
            System.err.println(e.getMessage());
        }

    }

}
