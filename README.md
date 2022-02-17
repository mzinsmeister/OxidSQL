# OxidSQL (Toy) SQL Database in Rust

I'm interested in databases and in Rust so i thought why not combine it and learn about 
databases and Rust by writing a SQL database in Rust. There's a working (although very limited)
database on the v1 branch which is based on the embedded Sled KV DB as a storage engine.
This is currently just a placeholder for where i hope a v2 will go one day. I am currently planning
on carrying at most the basic SQL parser in nom over to the new version and write my own storage 
manager. I will likely use a slotted page + B-Tree storage like most tratidional SQL databses do.

v1 can do:
- CREATE TABLE
- INSERT
- SELECT including joins (without the JOIN keyword)

The code of v1 is kinda hacked together and not very nice to read.
There is currently still some stuff especially in selects which i will likely fix before moving
on to v2. The query planner is basically the most primitive planner you can have. It has exactly
one optimization which is that it will do a primary key lookup when the query has exactly the form
"SELECT \[any colums\] FROM \[exactly one table\] WHERE \[primary key column\] = \[literal\]".
Everything else is handled by a simple combination of sequential scans and nested loop joins.
If there's a WHERE-clause it will be evaluated over the cross product of all tables.
The goal of this first version was first and foremost to produce a working database that can
actually do useful work without needing to programatically set some predefined state.
For the KV mapping i used something very similar on the way cockroachdb was doing it 
before they switched to their new layout. Transactions are currently not supported 
but once sled can actually do proper ACID transactions it would be kinda trivial to 
add i think. You could of course also implement MVCC at the KV level like i think 
cockroachdb also does.

The goals with v2 are:
- Write my own storage engine
- support all CRUD operations (maybe ALTER TABLE too)
- be multi user and transactional (i'm thinking likely MVCC)
- write a proper heuristics based optimizer/planner 
  with useable performance (maybe cost based later)
- make it an actual database server (maybe support postgres wire protocol)
- actually test it too

I will likely first implement the storage engine because you can test that in isolation pretty well.
I will also likely implement create table, insert and select first again and leave the rest for
later. All the multi user, transactional and server stuff or maybe a proper optimizer
will likely come last (since i found the planner/optimizer to be the part i found hardest to 
write in v1).
