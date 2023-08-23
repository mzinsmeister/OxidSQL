
/*
    I will later implement MVCC ACID transactions here. I will likely use an optimistic approach
    (Backwards oriented optimistic concurrency control) that is similar to the one described in
    "Fast Serializable Multi-Version Concurrency Control for Main-Memory Database Systems" 
    (https://db.in.tum.de/~muehlbau/papers/mvcc.pdf) or more specifically the disk based follow-up
    paper "Memory-Optimized Multi-Version Concurrency Control for Disk-Based Database Systems" 
    (https://db.in.tum.de/~freitag/papers/p2797-freitag.pdf?lang=en). This should be rather fast
    and allows me to avoid having to write a lock manager (which would be something between a PITA 
    and impossible to implement in a performant way in safe Rust). 

    One mistake that Postgres made that i will definitely not make is using 4-byte transaction ids.
 */