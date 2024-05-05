# RKNN Big Data Outlier Detection

This repository contains the code related to my final research project for obtaining my university title.

It's about anomaly detection techniques based on reverse k-nearest neighbors implemented on top of Scala and Apache Spark aiming for distributed/big data processing.

### Techniques implemented:
- Antihub and Antihub Refined (https://ieeexplore.ieee.org/abstract/document/6948273/)

### Search
The technique uses both results from k-nearest neighbors search and reverse k-nearest neighbors search. Since reverse neighbors can be found after k-nearest neighbors search, the latter is the focus of improvement.

An implementation for exhaustive search is provided.
Two pivot-based implementations are provided:
- One based on LAESA algorithm (https://www.sciencedirect.com/science/article/abs/pii/0167865594900957)
- One based on PkNN algorithm (https://link.springer.com/chapter/10.1007/978-3-319-71246-8_51)


Another implementation is ongoing development to make the search based on a kd-tree developed by Alex Boisvert (the entire kdtrey package)(https://github.com/aboisvert/kdtrey5).
