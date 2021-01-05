## MinHashing Articles

Below you can see details about the arguments needed to run ```min_hashing.py``` script:

```
> py min_hashing.py <inputDirectory> <outputDirectory> <runName> <numFeatures> <numHashTables> <distanceThreshold>
```

- ```inputDirectory```  - path to the directory that contains XML archives of the articles;
- ```outputDirectory``` - path to the directory that will persist:
  - MinHashLSH model;
  - articles table - it will contain features extracted from the body of the articles;
  - hash buckets table - hash-based partitioning of the articles;
  - Jaccard distance table - between each pair of articles;
- ```runName``` - identifier for the run;
- ```numFeatures``` - the number of features extracted from the body of the articles;
- ```numHashTables``` - the number of hash-functions used for LSH;
- ```distanceThreshold``` - upper bound for the Jaccard distance such as a pair of articles are considered similar.


Here is an example of running the script:
```
> cd .\source_files\
> py .\min_hashing.py "..\datasets\2016_testing_df\" "..\output\" test 20 2 0.6
```
After the jobs are finished the output should look like this: 
```
output/
____    test/
________    articles/
________    articles_jaccard_distances/    
________    articles_partitioned_by_hashes/
________    min_hash_model/
```