package com.zheyuan;

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
    private static String statement = host + "/sessions/1/statements";
    private static String sessions = host + "/sessions";
    private static OkHttpClient client = new OkHttpClient().newBuilder().build();
    public static Set<Integer> sessionIds = null;

    LivyClient(String host) throws IOException {
        this.host = host;
        if (sessionIds == null || sessionIds.isEmpty()) {
            assert (createSession().isSuccessful());
        }
        this.statement = sessions + "/" + sessionIds.iterator().next() + "/statements";
    }

    private static String parseFile(String path) throws IOException {
        ClassLoader classLoader = LivyClient.class.getClassLoader();

        File file = new File(Objects.requireNonNull(classLoader.getResource(path)).getFile());

        //File is found
        System.out.println("File Found : " + file.exists());

        //Read File Content
        return new String(Files.readAllBytes(file.toPath()));
    }

    static Response getSessionIds() throws IOException {
        if (sessionIds == null || sessionIds.isEmpty()) {
            sessionIds = new HashSet<Integer>();
        } else {
            sessionIds.clear();
        }
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
        }
        return response;
    }

    static Response createSession() throws IOException {
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "{\"kind\": \"scala\"}");
        Request request = new Request.Builder()
            .url(LivyClient.sessions)
            .method("POST", body)
            .addHeader("Content-Type", "application/json")
            .build();
        Response response = client.newCall(request).execute();
        System.out.println(response.body().string());
        assert (getSessionIds().isSuccessful());
        return response;
    }

    static Response deleteSession(int id) throws IOException {
        if (sessionIds == null || !sessionIds.contains(id)) {
            throw new IllegalArgumentException("Session with id: " + id + "does not exists");
        }

        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "");
        Request request = new Request.Builder()
            .url(LivyClient.sessions + "/" + id)
            .method("DELETE", body)
            .addHeader("Content-Type", "application/json")
            .build();
        Response response = client.newCall(request).execute();
        assert response.body() != null;
        assert getSessionIds().isSuccessful();
        System.out.println(response.body().string());
        return response;
    }

    static Response createStatementWithQuery(String query) throws IOException {

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

    static Response createStatementWithFile(Path path) throws IOException {

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
}
