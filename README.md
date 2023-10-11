# OxidSQL (Toy) SQL Database in Rust

<p align="center">
    <img src="logo/logo.png" alt="OxidSQL (Toy) SQL Database in Rust" width=200></img>
</p>

I'm interested in databases and in Rust so i thought why not combine it and learn about 
databases and Rust by writing a SQL database in Rust. There's a working (although very limited
and kinda hacked together) database on the v1 branch which is based on the embedded 
Sled KV DB as a storage engine.

The goals with v2 are:
- Write my own storage engine
- support all CRUD operations (maybe ALTER TABLE too)
- be multi user and transactional (i'm thinking likely MVCC)
- write a proper cost based based optimizer with a simple cost model (likely DPccp/DPhyp)
- make it an actual database server (maybe support postgres wire protocol)
- actually test it too

I will likely implement create table, insert and select first again and leave the rest for
later. All the multi user, transactional and server stuff will likely come last.

## Design principles
The main goal is to write a disk based system that can start out with a rather traditional 
architecture and then be used as a play-/testing-ground for me to implement all kinds of
cool new techniques (JIT compilation using cranelift, fancy new buffer managers, ...). 
Although the system is supposed to be disk-based, we live in a world
where RAM in the tens or even hundreds of GBs is ubiquitous. Therefore we can often just
assume that stuff fits into main memory. Especially for System- and Metadata we will mostly
not bother having the option of writing them to disk other than for persistence. For execution
we will for now also assume that we won't have to spool to disk (otherwise we could still rely
on OS swapping for this purpose for now).

## What currently works:
 - Simple CREATE TABLE
 - Simple INSERT (INSERT INTO x VALUES (a,b,c))
 - Simple select from where including simple predicates with <,>,>=,<=,= and 
     equi-joins in "attribute = attribute" style (currently only "AND" supported)
 - Analysis, planning and optimization (DPccp for now, will do DPhyp later)
 - Execution through a simple single threaded 
    volcano style interpreted engine printing the results to stdout (for now)
 - crashing and burning (no, but it panics) when someone enters a query that results in a cross product

## Current and upcoming TODOs:
 - Implement B+Tree based indexes
 - Replace self-written SQL parser by the quite advanced sqlparser crate
 - Try getting a postgres protocol based server to work with the pgwire crate
 - Implement some basic SELECT additions like GROUP BY/HAVING, LIMIT, ORDER aswell as UPDATE and DELETE (and DROP TABLE)
 - Properly integrate the HyperLogLog sketches that are already maintained on inserts into the optimizer especially for join selectivity estimation
 - Implement Recovery
 - Implement fast MVCC transactions
 - Maybe move everything over to async (at least evaluate it)
 - Maybe move everything over to optimistic/hybrid latches (probably would have to implement them myself first)
 - Maybe move to a more advanced buffer manager (evaluate a very optimized hash table vs vmcache and ways to handle variable size)
 - Implement a better execution engine (likely compiling)
