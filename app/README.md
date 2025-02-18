# Demo S2 Flink Jobs

## EventstreamJob

### Overview

Suppose we are running an e-commerce website and want to compute features for use by ranking models,
in real time, based on user interactions.

#### Raw input

Our input might consist of click or event stream data, tracking user journeys through the website.
Let's assume it has this form:

```
user=16;search=linen;epoch_ms=1739603607844
user=11;item=4;action=cart;epoch_ms=1739603608844
user=16;item=5;action=view;epoch_ms=1739603609844
user=5;search=gifts;epoch_ms=1739603610844
user=16;item=5;action=cart;epoch_ms=1739603611844
user=10;item=16;action=cart;epoch_ms=1739603612844
user=16;item=5;action=buy;epoch_ms=1739603613844
user=16;search=linen;epoch_ms=1739605707000
```

Every event has a userId, corresponding to the (logged-in) user that performed the action. There are
two types of events we are interested in: searches, which contain a query string that the user
issued; and interactions with products, which contain the interacted-with product id.

In the latter category, we might want to further distinguish between types of interactions with an
item -- view, cart add, buy, etc.

We will also assume that every event receives a timestamp.

#### Pattern matching

First, we need to impose some order onto this stream of events. We can start by extracting when a
query leads to a "conversion", or purchase, of an item. This is simply a tuple of
`(userId, itemId, searchQuery)`, that indicates that a particular user, after issuing a query, ended
up purchasing an item.

We only want to emit one of these query conversion tuples if we see a particular sequence of events
within our stream:

- user `A` performs a search with query `Q`
- user `A` views an item `I`
- user `A` adds item `I` to cart
- user `A` purchases item `I`

If we see that sequence of events occur within some time window, we can infer that the
query Q was relevant for A's purchase of I, and emit our query conversion data.

#### Featurizing

With a stream of query-item conversions, we can start creating features that might be relevant for a
ranking model. A simple item-level feature could be: the set of top-k query strings which ultimately
led to a conversion for the given item.

For example, for product with id `10`, let's assume that the majority of users who ended up
purchasing it originally found it via the query string "mid-century modern", and next most
frequently via "contemporary living":

```json
[
  10,
  {
    "top_queries": [
      "mid-century modern",
      "contemporary living",
      "fashion",
      "home and living",
      "kitchen"
    ]
  }
]
```

### Streaming features in Flink

The example job in Flink will let us produce these features from raw eventstream data in a streaming
fashion.

We'll assume that events get appended to S2 streams. They could all be collected within a single
stream, or across multiple (e.g., one stream per host that is collecting traffic). We'll assume the
latter for now.

The S2 flink source can be used to read from these streams, and do some initial processing of the
data.

After that, we can use Flink
SQL's [MATCH_RECOGNIZE](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/match_recognize/)
operation to extract sequences of events that lead to a conversion, and collect our intermediate
dataset into another S2 stream (though this is not strictly necessary), via the S2 flink sink.

Finally, we can define a dynamic table that computes the top-k converting queries, per item. This
can be backed by an upsert S2 table sink. Everytime there is an update to an item's set of top-k
queries, we will write an update to a S2 stream.

### Setup

Start by creating a basin, and some streams to work with.

```bash
export MY_BASIN="my-eventstream-demo-0001"
s2 create-basin "${MY_BASIN}"

# Create 10 host streams, for collecting raw eventstream inputs.
seq 0 9 \
	| xargs -I {} s2 create-stream "s2://${MY_BASIN}/host/000{}" --storage-class standard -r 1w
	
# Create a rollup stream for the intermediate query conversion dataset.
s2 create-stream "s2://${MY_BASIN}/rollup/converting-queries-per-item" --storage-class standard -r 1w

# Create a feature stream for collecting our computed features as upserts.
s2 create-stream "s2://${MY_BASIN}/feature/top-5-converting-queries-per-item" --storage-class standard -r 1w
```

Start the flink job. This can be done locally:

```bash
./gradlew runEventStreamJob
```

Or, you can export a fat JAR of the job and run it on something like AWS's managed Flink runtime.

While the job is running, generate some fake eventstream data with the provided utility. This will
generate a sequence of events, and append them randomly across the 10 `host` streams. It will also
write a collection of known query conversions to `/tmp/` directory, for manual verification with the
Flink-computed conversions.

```bash
./gradlew runEventSpoofer
```

If all is working properly, you should be able to see updates to the intermediate dataset:

```bash
s2 read s2://${MY_BASIN}/rollup/converting-queries-per-item
```

... and to the upsert table with the computed features:

```bash
# Format json is required to see the headers, which contain the key (userId) in upsert mode.
s2 read s2://${MY_BASIN}/feature/top-5-converting-queries-per-item --format json
```

For convenience, you can also visualize the result of applying the upserts in the feature stream
with a provided script:

```bash

# Grab current tail of the upsert stream, so that we can use that as a limit (and not continue
# tailing indefinitely).
tail=$(s2 check-tail s2://${MY_BASIN}/feature/top-5-converting-queries-per-item)
s2 read \
    s2://${MY_BASIN}/feature/top-5-converting-queries-per-item \
    --format json \
    --limit-count "${tail}" \
  | python scripts/eventstream-kv.py \
  | jq
```
