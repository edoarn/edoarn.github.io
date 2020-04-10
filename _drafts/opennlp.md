---
layout: post
title: Combining streams and text processing with Apache Flink and OpenNLP
subtitle: The unusual world of machine learning outside Python
tags: NLP streams
references:
    - id: 1
      title: Apache Flink's official site
      url: https://flink.apache.org
---

A spot on the podium for the most trending technologies in the current software industry should probably be reserved to real-time stream processing. In recent years, a plethora of tools and frameworks have seen the light, with many of them becoming part of the Apache Software Foundation, such as the popular Spark and his "little brother" Flink.
The former was initially conceived as a natural evolution of Hadoop with in-memory functionality, a general-purpose distributed computing tool on large amounts of data, but it has gradually been adapted for on-the-fly computation using mini-batches.
On the other hand, the latter was purposefully designed for stateful and distributed streams, becoming the perfect candidate to process information coming from different sources or services with minimal latency while keeping the overall system as much responsive as possible.

Apart from batches (which are still an option, by the way) Flink's _modus operandi_ is completely equivalent to Spark: each program can be seen as a single job that can be compiled, packaged in a jar file and executed in an environment, which in turn represents an abstraction for a cluster of physical or virtual machines sharing the work.
Leaving the details of the underlying architecture, well documented on the [official site](#ref-1), a single job is nothing more than a combination of basic building blocks called _operators_, starting from one or multiple sources and ending with at least a single output, called _sink_.

Having recently dealt with Twitter's streaming API, I took the opportunity to experiment with this excellent tool in its Java version and, if there was any doubt about Apache's software, I'm glad to report that it has been a pleasure, _for the most part_: