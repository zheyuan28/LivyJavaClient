# LivyJavaClient
Lightweight Java client for Livy

*For experimental use cases only*.

APIs:

1. getSessionIds: get current Livy Sessions
2. createSession: create a Livy Session with random session id
3. deleteSession: delete a Livy Session with specified id
4. createStatementWithQuery: create a simple query from String and submit to next available Livy session as statement
5. createStatementWithFile: create a simple query from file and submit to next available Livy session as statement

Zheyuan Zhao
Jan 28, 2020
