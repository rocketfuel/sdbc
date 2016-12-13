# Adding a dialect

The essence of SDBC is provided in the base package, and is two-fold:

1. Provide convenient ways to create queries.
2. Provide convenient ways of retrieving query results.

1 is provided by [ParameterizedQuery](base/src/main/scala/com/rocketfuel/sdbc/base/ParameterizedQuery.scala). It encapsulates some query, which is a string, with named parameters, and the parameter values, if any. A related trait, [ParameterValue](base/src/main/scala/com/rocketfuel/sdbc/base/ParameterValue.scala), provides an interface between the query and the underlying query framework.

2 is largely implementation dependent. If you are implementing a dialect on top of JDBC