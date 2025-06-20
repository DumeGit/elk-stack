package demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Request; import org.elasticsearch.client.Response; import org.elasticsearch.client.RestClient;

public class LowLevelReadExample {



  public static void main(String[] args) throws IOException {
    String basicAuth = Base64.getEncoder() .encodeToString("elastic:changeme".getBytes(
        StandardCharsets.UTF_8));
    RestClient llClient = RestClient.builder(new HttpHost("localhost", 9200, "http"))
        .setDefaultHeaders(new Header[]{ new BasicHeader("Authorization", "Basic " + basicAuth) })
        .build();

    Request req = new Request("GET", "/events/_search");
    req.setJsonEntity("{\"query\":{\"match_all\":{}}}");

    Response rsp = llClient.performRequest(req);

    StringBuilder textBuilder = new StringBuilder();
    try (Reader reader = new BufferedReader(new InputStreamReader
        (rsp.getEntity().getContent(), StandardCharsets.UTF_8))) {
      int c = 0;
      while ((c = reader.read()) != -1) {
        textBuilder.append((char) c);
      }
    }
    String json = textBuilder.toString();
    System.out.println(json);

    llClient.close();
  }
}