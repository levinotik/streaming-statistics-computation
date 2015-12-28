# What 

This repository contains a solution to "Problem 1: Streaming statistics computation" as described [here](https://www.wagonhq.com/challenge). 

# Build and Run

To build this project, use `stack build` then run the executable, specifiying the path to a CSV file, e.g. `.stack-work/dist/../build/wagon/wagon "test.csv"`. 

To view CPU and memory stats, use the `+RTS -s` flag. 

# Approach 

This solution uses the [Conduit](https://hackage.haskell.org/package/conduit) library for Haskell, which describes itself as 

> a solution to the streaming data problem, allowing for production, transformation, and consumption of streams of data in constant memory

Since the goal here is to be able to process a **lot** of data, we can't afford to have memory usage grow linearly with the number of rows. Conduit lets us keep memory usage constant. 

The basic implementation is to use the `csv-conduit` package which provides utilities for parsing csv files and streaming rows via Conduit. A `fold` is applied to the stream which generates the statistics. 

# Performance 

The main optimization was to use strictness annotations together with `force` in order to avoid building up a huge amount of unevaluated computations. Without `force` the memory usage quickly hits GB. With `force` we can keep the memory to 3MB or below. 

On my machine (2.2 GHz Intel Core i7 with 16GB memory), this implementation can process **10M csv rows using 3MB of memory**. Speed-wise, this implementation can process around **350K rows per second**. 

