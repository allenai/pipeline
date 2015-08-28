How to use Alternate Storage
=====

AIP support storage through the file system and, via the s3 sub-project, in Amazon S3. You can use any other kind of persistent storage by implementing a few classes.

# Artifact
You must create a class that extends the `Artifact` trait. The only required methods are `url`, which must uniquely idenfity an individual dataset, and `exists` which should return true only if the data exists in its entirety. Add other methods to your class as necessary to support reading and writing to storage.

# ArtifactFactory
You must create a class that instantiates your `Artifact` class from a URL. See `org.allenai.pipeline.s3.CreateCoreArtifacts` as an example. Then you can create a mix-in trait that Pipeline classes can mix in to be able to support your storage class.  See `org.allenai.pipeline.s3.S3Pipeline` for an example.

# ArtifactIo
You must implement an `ArtifactIo` class that can read/write objects to your storage, using the methods in your `Artifact` class.

With these classes in place, you can specify a URL corresponding to your storage in `Pipeline.rootOutputUrl` to use your storage for persistence of any Producers persisted via a `Pipeline.persist()` method.

