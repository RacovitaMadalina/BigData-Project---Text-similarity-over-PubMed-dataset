import findspark

findspark.init()
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.clustering import KMeans
import os
import sys

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext


def parse_paragraphs_list(list_of_paragraphs):
    list_of_paragraphs_to_ret = []
    if list_of_paragraphs is not None:
        for paragraph in list_of_paragraphs:
            if paragraph is not None:
                if type(paragraph) == str:
                    list_of_paragraphs_to_ret.append(paragraph)
                else:
                    if paragraph._VALUE is not None:
                        list_of_paragraphs_to_ret.append(paragraph._VALUE)
    return list_of_paragraphs_to_ret


def get_paragraphs_list_from_body(input_dir):
    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='record') \
        .options(rowTag='body') \
        .load(input_dir)
    return df.select("p") \
        .rdd \
        .map(lambda row: parse_paragraphs_list(row['p'])) \
        .zipWithIndex() \
        .map(lambda record: (record[1], record[0]))


def build_body(row_sec, row_p):
    concat_parsed_sections = ' '.join([' '.join(section) for section in parse_sections_list(row_sec)])
    concat_standalone_paragraphs = ' '.join(parse_paragraphs_list(row_p))
    #     print(concat_parsed_sections, concat_standalone_paragraphs)
    return concat_standalone_paragraphs + " " + concat_parsed_sections


def parse_sections_list(list_of_sections):
    if list_of_sections != None:
        list_of_sections = [parse_paragraphs_list(section.p) for section in list_of_sections if section.p != None]
    else:
        list_of_sections = []
    return list_of_sections


def get_sections_list(input_dir):
    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='record') \
        .options(rowTag='body') \
        .load(input_dir)
    return df.select("sec") \
        .rdd \
        .map(lambda row: parse_sections_list(row['sec']), ) \
        .zipWithIndex() \
        .map(lambda record: (record[1], record[0]))


def get_bodys_list(input_dir):
    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='record') \
        .options(rowTag='body') \
        .load(input_dir)
    return df.select("sec", "p") \
        .rdd \
        .map(lambda row: build_body(row['sec'], row["p"])) \
        .zipWithIndex() \
        .map(lambda record: (record[1], record[0]))


def get_abstract_list(input_dir):
    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='record') \
        .options(rowTag='abstract') \
        .load(input_dir)
    return df.select("sec", "p") \
        .rdd \
        .map(lambda row: build_body(row['sec'], row["p"])) \
        .zipWithIndex() \
        .map(lambda record: (record[1], record[0]))


def get_article_categories(row):
    categories = []
    for subj_group in row['subj-group']:
        if type(subj_group['subject']) == str:
            categories = [subj_group['subject']]
        elif type(subj_group['subject']) == list:
            for cat in subj_group['subject']:
                categories.append(cat)
    return categories


def get_categories_list(input_dir):
    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='record') \
        .options(rowTag='metadata') \
        .options(rowTag='article') \
        .options(rowTag='front') \
        .options(rowTag='article-meta') \
        .load(input_dir)

    return df.select("article-categories") \
        .rdd \
        .map(lambda row: get_article_categories(row['article-categories'])) \
        .zipWithIndex() \
        .map(lambda record: (record[1], record[0]))


def get_titles_list(input_dir):
    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='record') \
        .options(rowTag='metadata') \
        .options(rowTag='article') \
        .options(rowTag='front') \
        .options(rowTag='article-meta') \
        .options(rowTag='title-group') \
        .load(input_dir)

    return df.select("article-title") \
        .rdd \
        .map(lambda row: row['article-title']) \
        .zipWithIndex() \
        .map(lambda record: (record[1], record[0]))


def get_final_rdd(input_dir="../datasets/2016_testing_df/"):
    final_rdd = get_abstract_list(input_dir)
    final_rdd = final_rdd.join(get_bodys_list(input_dir))
    final_rdd = final_rdd.join(get_categories_list(input_dir))
    final_rdd = final_rdd.join(get_titles_list(input_dir))
    #     final_rdd = final_rdd.join(get_sections_list(input_dir))
    return final_rdd.sortByKey()


def get_final_to_pandas(input_dir="../datasets/2016_testing_df/"):
    return get_final_rdd(input_dir).map(
        lambda record: (record[1][0][0][0], record[1][0][0][1], record[1][0][1][0], record[1][1])) \
        .toDF(["abstract", "body", "categories", "title"])


def create_tfidf_features(sdf, num_features=100):
    sdf.registerTempTable('sdf')
    new_sdf = sqlContext \
        .sql("SELECT CONCAT(abstract, ' ', body, ' ', title) as text, categories as category from sdf")

    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    wordsData = tokenizer.transform(new_sdf)

    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=num_features)
    featurizedData = hashingTF.transform(wordsData)
    # alternatively, CountVectorizer can also be used to get term frequency vectors

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    return rescaledData.select('category', 'features')


def run_k_means(data, k=3):
    kmeans = KMeans(k=k)
    model = kmeans.fit(data.select('features'))
    return model.transform(data)


print('Usage: <input_dir> <output_dir> <n_clusters> <num_features>')
input_dir = sys.argv[1]  # "../datasets/2016_testing_df/"
output_dir = sys.argv[2]
n_clusters = int(sys.argv[3])
num_features = int(sys.argv[4])

sdf = get_final_to_pandas(input_dir=input_dir)
data = create_tfidf_features(sdf, num_features=num_features)
transformed_full = run_k_means(data, k=n_clusters)
transformed_full.write.option('path', os.path.join(output_dir, "kmeans_assignments")).saveAsTable(
    "kmeans_assignments_table")
