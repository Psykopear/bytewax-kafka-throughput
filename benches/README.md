## Scripts to benchmark

Each script in here is expected to export 3 variables:
- `ID`
- `CONSUME_TOPIC`
- `PRODUCE_TOPIC`

And an `if __name__ == '__main__':` block that executes the dataflow continuously.
The benchmarking suite will kill the process once it has collected enough data.
