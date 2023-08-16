
pub mod counting_hyperloglog;
pub mod sampling;

/*
    Since it's pretty clearly the superior strategy vs building histograms and also seems simpler
    to implement with good results in more cases, sampling plus sketches are going to be used as 
    the main method for estimating selectivities of predicates. In addition to that HyperLogLog 
    distinct count sketches will be used to estimate the number of distinct values in a column. 
    All of the statistics calculation is supposed to be updated online when tuples are 
    inserted/updated/deleted which should be possible with reasonably good performance according 
    to the literature (https://altan.birler.co/static/papers/2020DAMON_ConcurrentOnlineSampling.pdf
    for sampling). Also i don't want to implement the logic to regularly update the statistics in 
    the background. statistics will generally only be accumulated in memory (except for samples which
    will just be written into a regular heap segment and only written to
    disk at checkpoints/flush/shutdown. Otherwise there would be quite a performance impact for
    statistics gathering, also we're talking 
    
    Ironically the commercial system with the best cardinality estimation seems to be MS SQL Server 
    which uses histograms a lot (also sampling i guess?) as far as i know but as far as i know they 
    go to ridiculous lengths to get these results this way.

    These two methods can then be combined to get a good estimate of the selectivity of a predicate
    by:
    1. multiplying the number of hits by cardinality/#samples if there are any hits
    2. If there are no direct hits, try running parts of the predicate on the samples,
       use the highest selectivity found there and add the lower selectivities found
       with exponentially decaying weights
    3. if there are still no hits, use the 1/number of distinct values as an estimate for the
       selectivity of equi-predicates. for non equi-predicates we can at least assume the selectivity
       is at most 1/#samples

    https://www.cidrdb.org/cidr2020/papers/p25-moerkotte-cidr20.pdf
    https://dl.acm.org/doi/10.1145/3448016.3452805

    The cool thing about this is that this is not too hard to implement as soon as you have the online
    sampling and distinct count estimation and that it works on all kinds of predicates.

    Here's how to estimate group by cardinalities with this: https://db.in.tum.de/~freitag/papers/p23-freitag-cidr19.pdf
    This will not be implemented for now though (GB not even supported yet ;-)). Also group by is usually
    the last step processed in "normal" queries anyway and there won't be any optimization going on there
    for the time beeing. The only time you would need accurate cardinality estimates for group by is
    when you want to do joins with subqueries involving group by. Also this would only be relevant when
    you actually have larger group by results and only if you have at least 3 relations with one of them
    beeing such a subquery since with 2 only the left/right side of the join could be swapped but that 
    doesn't usually make an order of magnitude difference unless the cardinality difference between 
    the two sides is extremely huge.

    To estimate join-predicate selectivities the base case (which can just be used for everything in 
    the beginning) is:
    1. Join the samples, if there are hits, use that for the estimate
    2. If there are no hits, assume independence and use the minimum of the input #unique estimates
       as the cardinality estimate for the join result (assume every tuple on the right actually matches
       with one tuple on the right). For non-equi-joins i guess the best way to do this then is to just
       pretend it's an equi-join in this case and use the same method. This is probably then not too far 
       off from the actual result since if it was closer to a cross product the first method would have found
       some hits. Also if you use non-equi joins you deserve to suffer anyway ;-)
    
    If you have indexes on one of the sides in an equi-join, you can use the index to get a better
    estimate for the cardinality of the join result. This is done by: https://www.cidrdb.org/cidr2017/papers/p9-leis-cidr17.pdf

    Generally the findings of this paper should be followed: https://www.vldb.org/pvldb/vol9/p204-leis.pdf

    Since the data structures maintained for the statistics are in-memory and will
    only be written to disk for checkpoints or on (regular) shutdown we will have to
    take some kind of exclusive lock on all modifications to stop all modifications to the database
    for a very short amount of time while we take a consistent snapshot of our statistics data-structures
    that we then write to disk. This should be fine since this will only happen every few minutes and will
    only take a few microseconds to milliseconds. Another possibility would be just writing it on orderly
    shutdown and rebuilding on recovery.
 */