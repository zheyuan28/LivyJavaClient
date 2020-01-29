import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.JSONArray;
import org.json.JSONObject;


class LivyClient {

    private static String host = "http://den03cyq.us.oracle.com:8998";
    private static String statement = host + "/sessions/5/statements";
    private static String sessions = host + "/sessions";
    private static OkHttpClient client = new OkHttpClient().newBuilder().build();
    private static Set<Integer> sessionIds = null;

    LivyClient() throws IOException {
        if (LivyClient.getSessionIds().isEmpty()) {
            createSession();
        }
    }

    private static String parseFile(String path) throws IOException {
        ClassLoader classLoader = LivyClient.class.getClassLoader();

        File file = new File(Objects.requireNonNull(classLoader.getResource(path)).getFile());

        //File is found
        System.out.println("File Found : " + file.exists());

        //Read File Content
        return new String(Files.readAllBytes(file.toPath()));
    }

    private static Set<Integer> getSessionIds() throws IOException {
        if (sessionIds == null || sessionIds.isEmpty()) {
            sessionIds = new HashSet<Integer>();
        } else {
            sessionIds.clear();
        }

        OkHttpClient client = new OkHttpClient().newBuilder()
            .build();
        Request request = new Request.Builder()
            .url(LivyClient.sessions)
            .method("GET", null)
            .addHeader("Content-Type", "application/json")
            .build();
        Response response = client.newCall(request).execute();

        assert response.body() != null;
        JSONObject jsonObject = new JSONObject(response.body().string());
        JSONArray arr = jsonObject.getJSONArray("sessions");

        for (int i = 0; i < arr.length(); i++) {
            int sessionId = arr.getJSONObject(i).getInt("id");
            sessionIds.add(sessionId);
            System.out.println("found session: " + sessionId);
        }
        return sessionIds;
    }

    private static Response createSession() throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder()
            .build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "{\"kind\": \"scala\"}");
        Request request = new Request.Builder()
            .url(LivyClient.sessions)
            .method("POST", body)
            .addHeader("Content-Type", "application/json")
            .build();
        Response response = client.newCall(request).execute();
        System.out.println(response.body().string());
        return response;
    }

    private static Response deleteSession(int id) throws IOException {
        if (!getSessionIds().contains(id)) {
            throw new IllegalArgumentException("Session with id: " + id + "does not exists");
        }

        OkHttpClient client = new OkHttpClient().newBuilder()
            .build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "");
        Request request = new Request.Builder()
            .url(LivyClient.sessions + "/" + id)
            .method("DELETE", body)
            .addHeader("Content-Type", "application/json")
            .build();
        Response response = client.newCall(request).execute();
        assert response.body() != null;
        System.out.println(response.body().string());
        return response;
    }

    private static Response createStatementWithQuery(String query) throws IOException {

        MediaType mediaType = MediaType.parse("application/json");
        JSONObject obj = new JSONObject();
        obj.put("code", query);
        RequestBody body = RequestBody.create(mediaType, obj.toString());
        Request request = new Request.Builder()
            .url(LivyClient.statement)
            .method("POST", body)
            .addHeader("Content-Type", "application/json")
            .build();
        Response response = client.newCall(request).execute();
        assert response.body() != null;
        System.out.println(response.body().string());
        return response;
    }

    private static Response createStatementWithFile(Path path) throws IOException {

        MediaType mediaType = MediaType.parse("application/json");
        String bodyContent = LivyClient.parseFile(path.toString());
        JSONObject obj = new JSONObject();
        obj.put("code", bodyContent);

        RequestBody body = RequestBody.create(mediaType, obj.toString());
        Request request = new Request.Builder()
            .url(LivyClient.statement)
            .method("POST", body)
            .addHeader("Content-Type", "application/json")
            .build();
        Response response = client.newCall(request).execute();
        assert response.body() != null;
        // debug
        System.out.println(response.body().string());
        return response;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
//        createStatementWithFile(Paths.get("query1.scala"));
        int i = 2;
        do {
            createStatementWithQuery("12 + 3");
            Thread.sleep(1000L);
        } while (i-- > 0);
    }
}
