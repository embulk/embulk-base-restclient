package org.embulk.util.retryhelper.jetty92;

import com.sun.net.httpserver.HttpServer;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.EmbulkTestRuntime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Jetty92RetryHelperTest
{
    @Rule
    public final EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private HttpServer httpServer;

    @Before
    public void setUp() throws IOException
    {
        httpServer = HttpServer.create(new InetSocketAddress(8087), 0);
        httpServer.createContext("/test", exchange -> {
            byte[] response = "{\"success\": true}".getBytes();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        httpServer.start();
    }

    @After
    public void tearDown()
    {
        httpServer.stop(1);
    }

    @Test
    public void requestWithRetry() throws Exception
    {
        int maxRetry = 3;

        Jetty92RetryHelper retryHelper = new Jetty92RetryHelper(maxRetry,
                10000,
                10000,
                new DefaultJetty92ClientCreator(1000, 10000));

        String responseBody = retryHelper.requestWithRetry(
                new StringJetty92ResponseEntityReader(10000),
                new Jetty92SingleRequester()
                {
                    int retryCnt = 0;

                    @Override
                    public void requestOnce(HttpClient client, Response.Listener responseListener)
                    {
                        Request req;

                        if (retryCnt < 1) {
                            req = client.newRequest("http://localhost:8087/atest");
                        } else {
                            req = client.newRequest("http://localhost:8087/test");
                        }
                        retryCnt++;
                        req.timeout(10000, TimeUnit.MILLISECONDS)
                                .idleTimeout(10000, TimeUnit.MILLISECONDS)
                                .method(HttpMethod.GET)
                                .send(responseListener);
                    }

                    @Override
                    protected boolean isResponseStatusToRetry(Response response)
                    {
                        if (response.getStatus() == 404) {
                            return true;
                        }
                        return false;
                    }

                    @Override
                    protected boolean isExceptionToRetry(Exception exception)
                    {
                        return exception instanceof EOFException || exception instanceof TimeoutException;
                    }
                });

        assertTrue(!responseBody.isEmpty());
        assertEquals("{\"success\": true}", responseBody);
    }
}
