Data Organization for Collaborative Experimentation
=====

This document describes a recommended setup for using AllenAI Pipeline
for collaborative data science work.

There are 4 fundamental types of resources that must be managed in a collaborative effort:

 1. Code
 1. Source Data
 1. Derived Data
 1. Reports

Code
-----
Code is the source code containing logic for transforming data.
It can be kept in any source-control system.

Source Data
-----
Source data is any exogenous dataset, i.e. any data set that was not created
by the code in our project's source-control repo.  The origins of source data
are not tracked by the pipeline, they are assumed to be manually uploaded to some shared
location.  The shared location must be identified by a stable, absolute URL, where "absolute" means
that the same URL will resolve to the same resource on all users' machines and "stable" means
that the contents behind a given URL will never change. If a different variant of
a source dataset is created, it should be given a different URL when uploaded.

Derived Data
-----
Derived data is data that is computed by the code under the project's source control.
The precise naming of a particular dataset is handled by the pipeline library. The user-supplied
name is appended with a unique hash code derived from the data and code that feeds into that dataset.
The root output location of derived data is specified by the `Pipeline.rootOutputUrl` field.

Reports
-----
A report is a historical record of a pipeline that been executed.  AllenAI Pipeline creates a set of files
when `Pipeline.run()` is executed.  These files should be written to a shared location or stored in source control.
Users can browse the collection of reports to trace the lineage of any dataset in the derived data, and to
create variants of one another's pipeline runs by branching the code.  The mechanism of assigning
semantic version numbers to components allows for pipelines run using code from different source control branches
to nevertheless share derived data in a consistent way.

