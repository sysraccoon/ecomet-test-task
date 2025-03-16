CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.repositories
(
    name     String,
    owner    String,
    stars    Int32,
    watchers Int32,
    forks    Int32,
    language String,
    updated  datetime
) ENGINE = ReplacingMergeTree(updated)
      ORDER BY name;

CREATE TABLE IF NOT EXISTS test.repositories_authors_commits
(
    date        date,
    repo        String,
    author      String,
    commits_num Int32
) ENGINE = ReplacingMergeTree
      ORDER BY (date, repo, author);

CREATE TABLE IF NOT EXISTS test.repositories_positions
(
    date     date,
    repo     String,
    position UInt32
) ENGINE = ReplacingMergeTree
      ORDER BY (date, repo);
