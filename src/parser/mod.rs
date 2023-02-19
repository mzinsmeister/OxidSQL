/*
    TODO: Implement a parser for the SQL dialect supported by the database (likely using the nom
          parser combinator library). The parser should produce an abstract syntax tree (AST) that 
          can be analyzed by the analyzer which for now will also live in this module. The SQL
          dialect should for now be compatible with postgres.

          Options are:
            - Use the parser written for my hacky v1 database (also with Nom)
            - Use a library exposing the actual postgres parser in Rust
            - Write a parser from scratch
 */