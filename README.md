# RKNN Big Data Outlier Detection

Anomaly detection techniques based on reverse k-nearest neighbors implemented on top of Scala and Apache Spark aiming for big data processing.

### Techniques implemented:
- Antihub and Antihub Refined (https://ieeexplore.ieee.org/abstract/document/6948273/)

### Search
The technique uses both results from k-nearest neighbors search and reverse k-nearest neighbors search. Since reverse neighbors can be found after k-nearest neighbors search, the latter is the focus of improvement.

An implementation for exhaustive search is provided.
Another implementation is ongoing development to make the search based on a kd-tree developed by Alex Boisvert (the entire kdtrey package)(https://github.com/aboisvert/kdtrey5).
