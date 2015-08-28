#Allen-AI Pipeline Framework

The Allen-AI Pipeline (AIP) framework is a library that facilitates collaborative on data-driven
projects. It allows users to define workflows that share data resources transparently while maintaining
complete freedom over the environment in which those workflows execute. AIP falls somewhere between
unix make and KNIME. Unlike make, it can operate on cloud storage resources and execute in a
distributed environment. Unlike KNIME, it does not lock you into any particular repository for storing
your data or execution environment in which the workflows must run.

AIP can be used in two ways:

1. As [PipeScript](docs/PipeScript.md), a binary that interprets a simple scripting language
to execute native commands locally and store the results remotely
2. Via the [Scala API](docs/README.md) to define workflows that produce strongly-typed objects and
execute within any JVM-based environment.

In summary, AIP provides the following benefits:

- Intermediate data is cached and is sharable by different users on different systems.
- A record of past runs is maintained, with navigable links to all inputs/output of the pipeline.
- A pipeline can be visualized before running.
- Output resource naming is managed to eliminate naming collisions.
- Input/output data is always compatible with the code reading the data.

Send questions to *rodneyk@allenai.org*


















