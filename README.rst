arangodb
========

Objective
---------

There are a couple of Python implementations for ArangoDB's rest API, but they
do not fit my mind:

* Have a declarative way of defining documents, Edges and Graphs.
* Have a clean separation of client API and ArangoDB API.
* Documents should be optionally de/serializable when loading or saving.
* The API client may differ for a special Document.
* Documents should behave like dicts.
* Have a nice query tool.
* Have ArangoDB errors thrown as exceptions.
* Use requests Session with keep-alive.


