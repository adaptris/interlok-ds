# Interlok JDBC

## JDBC Statement Buidler

The statement builder (both capture and query) aims to simplify the
syntax when writing SQL statements. Part of its goals are to make
statement-parameters redundant and thus we needed a new syntax:

    INSERT INTO hits (reference, message_id, id, entity_id, blocking) VALUES
    (%sql_metadata{string:reference}, %sql_metadata{string:%uniqueId}, %sql_metadata{string:id}, %sql_metadata{string:entity_id}, %sql_metadata{string:blocking})

Where under the covers we actually turn that into `INSERT INTO hits
(reference, message_id, id, entity_id, blocking) VALUES (?, ?, ? ,? ,?)`
and then generate a list of statement parameters automatically:

```xml
<jdbc-string-statement-parameter>
  <query-string>reference</query-string>
  <query-type>metadata</query-type>
</jdbc-string-statement-parameter>
<jdbc-string-statement-parameter>
  <query-string>message_id</query-string>
  <query-type>id</query-type>
</jdbc-string-statement-parameter>
<jdbc-string-statement-parameter>
  <query-string>id</query-string>
  <query-type>metadata</query-type>
</jdbc-string-statement-parameter>
<jdbc-string-statement-parameter>
  <query-string>entity_id</query-string>
  <query-type>metadata</query-type>
</jdbc-string-statement-parameter>
<jdbc-string-statement-parameter>
  <query-string>blocking</query-string>
  <query-type>metadata</query-type>
</jdbc-string-statement-parameter>
<statement>INSERT INTO hits (reference, message_id, id, entity_id, blocking) VALUES (?, ?, ? ,? ,?);</statement>
```
