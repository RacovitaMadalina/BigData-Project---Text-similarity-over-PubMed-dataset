import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, collect_set
from pyspark.sql.types import IntegerType, ArrayType, FloatType
from pyspark.ml.feature import MinHashLSH, Tokenizer, RegexTokenizer, StopWordsRemover, HashingTF, IDF

import re
import os
import sys

from data_parsing import get_simplified_to_df


def getArticlesFeaturesFromBody(articlesDataFrame, numFeatures = 20) :
    regexTokenizer = RegexTokenizer(inputCol = "body", outputCol = "words", pattern = "\\W")
    remover = StopWordsRemover(inputCol = "words", outputCol=  "filtered")
    hashingTF = HashingTF(inputCol = "filtered", outputCol = "rawFeatures", numFeatures = numFeatures)
    idf = IDF(inputCol = "rawFeatures", outputCol = "features")
    
    countTokens = udf(lambda words: len(words), IntegerType())

    tokenizedDataFrame = \
        regexTokenizer \
            .transform(articlesDataFrame)

    filteredDataFrame= \
        remover \
            .transform(tokenizedDataFrame) \
            .withColumn("tokens", countTokens(col("filtered")))

    featurizedDataFrame = hashingTF.transform(filteredDataFrame)
    idfModel = idf.fit(featurizedDataFrame)
    rescaledDataFrame = idfModel.transform(featurizedDataFrame)

    return rescaledDataFrame

def getArticlesBucketsByHashes(rescaledDataFrame, mhModel) :
    minHashedDataFrame = \
        mhModel \
            .transform(rescaledDataFrame)

    bucketsDataFrame = \
        minHashedDataFrame \
            .groupBy("hashes") \
            .agg(collect_set("id").alias("ids"),
                 collect_set("title").alias("titles"))

    return bucketsDataFrame

def getArticlesJaccardDistances(rescaledDataFrame, mhModel, distanceThreshold = 0.5) :
    jaccardDistancesDataFrame = \
        mhModel \
            .approxSimilarityJoin(rescaledDataFrame, rescaledDataFrame, threshold = distanceThreshold, distCol = "JaccardDistance") \
            .select(col("datasetA.id").alias("id1"),
                    col("datasetB.id").alias("id2"),
                    col("jaccardDistance")) \
            .filter("`id1` != `id2`") \
            .orderBy(col("jaccardDistance"))

    return jaccardDistancesDataFrame

if __name__ == '__main__' :

    noOfArguments = len(sys.argv)

    if noOfArguments < 4 :
        print("Usage: py " + os.path.basename(__file__) + " <inputDirectory> <outputDirectory> <runName> <numFeatures> <numHashTables> <distanceThreshold> \n", )
        sys.exit()

    inputDirectory = sys.argv[1]
    outputDirectory = sys.argv[2]
    runName = sys.argv[3]
    numFeatures = int(sys.argv[4]) if noOfArguments >= 5 else 20
    numHashTables = int(sys.argv[5]) if noOfArguments >= 6 else 2
    distanceThreshold = float(sys.argv[6]) if noOfArguments == 7 else 0.5

    outputPath = os.path.join(outputDirectory, runName)

    conf = SparkConf().setMaster("local[*]").setAppName("MinHashing Articles")
    sc = SparkContext(conf = conf)
    spark = SparkSession(sc)

    articlesDataFrame = get_simplified_to_df(spark, inputDirectory)
    rescaledDataFrame = getArticlesFeaturesFromBody(articlesDataFrame, numFeatures = numFeatures)

    mh = MinHashLSH(inputCol = "features", outputCol = "hashes", numHashTables = numHashTables)
    mhModel = mh.fit(rescaledDataFrame)

    bucketsDataFrame = getArticlesBucketsByHashes(rescaledDataFrame, mhModel)
    jaccardDistancesDataFrame = getArticlesJaccardDistances(rescaledDataFrame, mhModel, distanceThreshold = distanceThreshold)

    rescaledDataFrame.write.option('path', os.path.join(outputPath, "articles")).saveAsTable("articles")
    mhModel.save(os.path.join(outputPath, "min_hash_model"))
    bucketsDataFrame.write.option('path', os.path.join(outputPath, "articles_partitioned_by_hashes")).saveAsTable("articles_partitioned_by_hashes")
    jaccardDistancesDataFrame.write.option('path', os.path.join(outputPath, "articles_jaccard_distances")).saveAsTable("articles_jaccard_distances")