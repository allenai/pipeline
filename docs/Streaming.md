How to Handle Streaming Data
=====

In general, AIP is not a streaming system like [Storm](https://storm.apache.org/). It assumes that all steps in a pipeline are deterministic and that data can be persisted and re-used multiple times transparently.  That said, AIP can handle data that is too large to fit into RAM if a Producer's output is of type `Iterator[_]`. When saved, such data will be written to disk without being stored in memory. If a Producer of `Iterator` is persisted, it will write all its data to disk before passing the result to any downstream consumers, each of which will read the data from disk. If a Producer of `Iterator` is not persusted and is consumed by multiple downstream consumers, it will recompute its output for every consumer.

If you have data types that are similar to `Iterator`, in that they can only be consumed once, then any Producer of that type should mix in the `WithCachingDisabled` trait (or use the `withCachingDisabled` method after construction) to ensure the same behavior.

