# infinitydb
The open-source REST client access code for the InfinityDB
NoSQL Java DBMS at boilerbay.com. InfinityDB is even more
flexible than 'JSON document' databases without sacrificing the
critical RDBMS features like transactions, joins, immediate
consistency, referential integrity and more.

There is helper code for Java, JavaScript, and Python for REST, but
you can do it directly as well. The python can be imported with
python3 -m pip install --upgrade infinitydb. Keep your pip
up to date as well.

Infinitydb is at boilerbay.com in the form 
of either a complete server InfinityDB Server, or as a Java jar
with InfinityDB Embedded or InfinityDB Encrypted. In the
Amazon Web Services Marketplace you can subscribe and 
launch InfinityDB Server instances and then edit data and
administer the server via web and do REST requests.

The Server has a flexible declarative PatternQuery 
language that is easy to use to define REST APIs with no 
addtional server-side code.  PatternQuery is much
more powerful than SQL, and approaches regular code
in capability. REST APIs can be kept forwards-compatible
because PatternQueries conceal database structure and
implementation in an Object-Oriented style.

The Schemas, APIs, queries, data, and administration are web-based
and all are dynamic, meaning that changes show up instantly. 
Schemas are embedded in the data itself for instant extension
as new forms of data arrive.

There is also a fast binary protocol to remote servers for
distributed access called 'ItemPacket'.

All server access is TLS protected. There are users, permissions,
roles, grants, databases, and fine-grained query
interface permissions.
