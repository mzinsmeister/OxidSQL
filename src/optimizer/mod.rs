/*
    TODO: Implement an optimizer that can take a query plan and produce an optimized plan by applying
          optimization techniques such as:#
            - Join ordering
            - Index selection
            - Constant folding (maybe also in the planner)
            - Predicate pushdown(maybe also in the planner)
            - etc.

          The optimizer should also be able to produce a cost estimate for the plan. The main task
          for the first version of the optimizer will be join ordering. For now i'm planning on 
          implementing a version of Thomas Neumanns adaptive query optimization 
          (https://db.in.tum.de/~radke/papers/hugejoins.pdf), maybe just without
          the intermediate linDP (or probably the linDP++) algorithm in the beginning 
          which can still be added later while still giving us a nice graceful degradation of quality
          of resulting plans the more join-relations the query contains while keeping a reasonable 
          optimization time.
 */