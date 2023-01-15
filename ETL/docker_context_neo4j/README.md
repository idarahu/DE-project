Build:

```zsh
$ docker build -t neo4j-script -f neo4j-script.dockerfile .
```

Run with `zsh`:

```zsh
$ docker run -v "`(pwd)`/neo4j/import/:/import" -v "`(pwd)`/neo4j/plugins:/plugins" -p 7474:7474 -p 7687:7687 -it --rm neo4j-script
```