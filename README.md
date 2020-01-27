# pyspark-kit
Attempt to create a sensible pyspark structure boilerplate. Heavily work in
progress so it's constantly breaking

# Build Tool
[Bazel](https://bazel.build/), was the chosen one even after agonizing pains of
researching and prototyping alternatives (Buck, Pants). It still have its
quirks, but *insert Thanos memes*
> "You could not live with your own failure."
> "Where did that bring you? Back to me."

# Batteries
## Spark Pytest Helper

## Spark Functional Tools
- `chain_transforms`: compose multiple transforms on a DataFrame

## Spark Patches
- `df.transforms` API is added to follow the Scala API

# Requirements

- Bazel 2.0.0
- Grit & Patience (trust me, you need it. I do :C)
