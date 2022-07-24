# Web indexer and query processor

This repo includes:

- Indexer of documents stored in WARC format
- A query processor

The Indexer outputs an index in a specific format that the query processor
understands. The query processor takes the index and a set of queries as
parameters, and outputs a document rank of the documents for each query.

## Execution instructions

### Indexer

Execute the indexer as follows:

```shell
python3 indexer.py -m <MEMORY> -c <CORPUS> -i <INDEX>
```

For example, to execute the indexer with a memory limit of 1024 MB, using the
corpus files contained in the `data/corpus` directory, and output the index to
`index.out`, run:

```shell
python3 indexer.py -m 1024 -c data/corpus -i index.out
```

### Query processor

Execute the query processor as follows. The parameter `<INDEX>` is the file
output by the Indexer.

```shell
python3 processor.py -i <INDEX> -q <QUERIES> -r <RANKER>
```

For example, to execute the query processor using the index `index.out`, the
queries in the file `queries-sample.txt` and the ranker `BM25`, run:

```shell
python3 processor.py -i index.out -q queries-sample.txt -r BM25
```

The available rankers are `BM25` AND `TFIDF`.
