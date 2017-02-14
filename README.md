embulk-base-restclient
=======================

Base class library to access RESTful services. See [an example](embulk-input-example/).

Versions and compatibility
---------------------------

Please remember to specify a version of this library when you use it. This is just a helper library to build a plugin. The interfaces may change per library version for improvements, optimization, and catch-up.

Usage: input plugins
---------------------

### Overview

The easiest case has two classes: `-InputPlugin` and `-InputPluginDelegate` as follows.

```
public class -InputPlugin
        extends RestClientInputPluginBase<-InputPluginDelegate.PluginTask>
{
    public -InputPlugin()
    {
        super(-InputPluginDelegate.PluginTask.class, new -InputPluginDelegate());
    }
}
```

```
public class -InputPluginDelegate
        implements RestClientInputPluginDelegate<-InputPluginDelegate.PluginTask>
{
    public interface PluginTask
            extends RestClientInputTaskBase
    {
    }

    .........
}
```

`-InputPlugin` is just to delegate to `-InputPluginDelegate`. `-InputPluginDelegate` is to implement actual behaviors. `-InputPluginDelegate` is a set of some `abstract` methods which are called implicitly from `-InputPlugin`.


### Key method 1: `buildServiceResponseMapper`

It defines 1) which values to pick up from the response, and 2) which Embulk columns to import the picked-up values. Return an instance of `org.embulk.base.restclient.jackson.JacksonServiceResponseMapper` if the target RESTful service responds with JSON. Implement another type of `ServiceResponseMapper` if the response is not in JSON (e.g. XML).

`JacksonServiceResponseMapper` is usually built as follows:

```
return JacksonServiceResponseMapper.builder()
    .add(...)
    .add(...)
    ...
    .build();
```

One `.add(...)` represents one value imported into one Embulk column. There are a few overloaded `add` methods.

* To pick up a top-level JSON value into an Embulk column with the same name, use the simplest `add(String, org.embulk.spi.type.Type)`.
* To pick up into an `Timestamp` Embulk column, use `add` with `embulkColumnTimestampFormat`.
* To pick up a non-top-level JSON value, or a value into an Embulk column with a different name, use `add` with `JacksonValueLocator`. `JacksonValueLocator` is to specify a location in JSON. For example, `JacksonJsonPointerValueLocator` points a location in JSON with [JSON Pointer](https://tools.ietf.org/html/rfc6901).

From the following JSON for example:

```
{
    "fullname": "example_name",
    "timestamps": {
        "start": 1487056476,
        "end": 1487056536
    }
}
```

* `add("fullname", Type.STRING)` picks up a `String` value `"example_name"` into an Embulk column `"fullname"` .
* `add(new JacksonJsonPointerValueLocator("/timestamps/start"), "starting_time", Type.TIMESTAMP, "%s")` picks up a `Timestamp` value `2017-02-14T07:14:36Z` (`1487056476`) into an Embulk column `"starting_time"`.

### Key method 2: `ingestServiceData`

It actually retrieves data from the target RESTful service, and imports the data as Embulk records. Given `RecordImporter` imports one record into Embulk based on the `ServiceResponseMapper` generated above.

Given `RetryHelper` encapsulates complicated retrying implementation. Its usage is in three steps as follows:

```
String responseBody = retryHelper.requestWithRetry(
    new org.embulk.base.restclient.request.StringResponseEntityReader(),
    new org.embulk.base.restclient.request.SingleRequester() {
        @Override
        public javax.ws.rs.core.Response requestOnce(javax.ws.rs.client.Client client)
        {
            // Returns a JAX-RS Response retrieve with the given JAX-RS Client.
            return client...get();
        }

        @Override
        public boolean isResponseStatusToRetry(javax.ws.rs.core.Response response)
        {
            return response.getStatus() / 100 != 4;
        }
    });
```

1. Request to the target RESTful service by implementing `requestOnce`. It returns JAX-RS Response `javax.ws.rs.core.Response`.
2. Decice whether the request should be retried or not by implementing `isResponseStatusToRetry`. It decies with the `Response` (usually based on the HTTP response status).
3. Achieve the response body entity from the JAX-RS Response by specifying a `ResponseReadable` instance to `requestWithRetry`. A couple of `ResponseReadable` classes are pre-defined:
  * `StringResponseEntityReader` to read the entity as `String`.
  * `InputStreamResponseEntityReader` to read the entity as `InputStream`.
