import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

    private static final String GET_URL = "";// insert get url
    private static final String POST_URL = "";// insert post url
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpGet request = new HttpGet(GET_URL);
            CloseableHttpResponse response = httpClient.execute(request);
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            JsonArray callData = gson.fromJson(reader, JsonArray.class);
            response.close();

            Map<String, Map<String, List<JsonObject>>> groupedCalls = new HashMap<>();

            for (JsonElement elem : callData) {
                JsonObject call = elem.getAsJsonObject();
                int customerId = call.get("customerID").getAsInt();
                long startTimestamp = call.get("startTimestamp").getAsLong();
                long endTimestamp = call.get("endTimestamp").getAsLong();

                String date = convertToUtcDate(startTimestamp);
                String customerKey = String.valueOf(customerId);

                groupedCalls.putIfAbsent(customerKey, new HashMap<>());
                groupedCalls.get(customerKey).putIfAbsent(date, new ArrayList<>());
                groupedCalls.get(customerKey).get(date).add(call);
            }

 
            List<JsonObject> results = new ArrayList<>();
            for (String customerId : groupedCalls.keySet()) {
                for (String date : groupedCalls.get(customerId).keySet()) {
                    List<JsonObject> calls = groupedCalls.get(customerId).get(date);
                    List<long[]> events = new ArrayList<>();

                    for (JsonObject call : calls) {
                        events.add(new long[]{call.get("startTimestamp").getAsLong(), 1});
                        events.add(new long[]{call.get("endTimestamp").getAsLong(), -1});
                    }

                    events.sort(Comparator.comparingLong(a -> a[0]));

                    int concurrentCalls = 0, maxConcurrentCalls = 0;
                    long timestampForMax = 0;
                    List<String> callIdsForMax = new ArrayList<>();
                    List<String> ongoingCallIds = new ArrayList<>();

                    for (long[] event : events) {
                        if (event[1] == 1) {
                            concurrentCalls++;
                            JsonObject call = findCallByTimestamp(calls, event[0]);
                            ongoingCallIds.add(call.get("callId").getAsString());

                            if (concurrentCalls > maxConcurrentCalls) {
                                maxConcurrentCalls = concurrentCalls;
                                timestampForMax = event[0];
                                callIdsForMax = new ArrayList<>(ongoingCallIds);
                            }
                        } else {
                            concurrentCalls--;
                            ongoingCallIds.remove(findCallByEndTimestamp(calls, event[0]).get("callId").getAsString());
                        }
                    }

         
                    JsonObject result = new JsonObject();
                    result.addProperty("customerID", Integer.parseInt(customerId));
                    result.addProperty("date", date);
                    result.addProperty("maxConcurrentCalls", maxConcurrentCalls);
                    result.addProperty("timestamp", timestampForMax);
                    JsonArray callIdsArray = new JsonArray();
                    for (String callId : callIdsForMax) {
                        callIdsArray.add(callId);
                    }
                    result.add("callIDs", callIdsArray);
                    results.add(result);
                }
            }

  
            HttpPost postRequest = new HttpPost(POST_URL);
            StringEntity entity = new StringEntity(gson.toJson(Collections.singletonMap("results", results)), StandardCharsets.UTF_8);
            postRequest.setEntity(entity);
            postRequest.setHeader("Content-type", "application/json");

            CloseableHttpResponse postResponse = httpClient.execute(postRequest);
            System.out.println(postResponse.getCode() + ": " + postResponse.getReasonPhrase());
            postResponse.close();
            httpClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static String convertToUtcDate(long timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date(timestamp));
    }

    private static JsonObject findCallByTimestamp(List<JsonObject> calls, long timestamp) {
        for (JsonObject call : calls) {
            if (call.get("startTimestamp").getAsLong() == timestamp) {
                return call;
            }
        }
        return null;
    }


    private static JsonObject findCallByEndTimestamp(List<JsonObject> calls, long timestamp) {
        for (JsonObject call : calls) {
            if (call.get("endTimestamp").getAsLong() == timestamp) {
                return call;
            }
        }
        return null;
    }
}
