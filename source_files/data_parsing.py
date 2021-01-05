from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def parse_paragraphs_list(list_of_paragraphs):
    list_of_paragraphs_to_ret = []
    if list_of_paragraphs != None:
        for paragraph in list_of_paragraphs:
            if paragraph != None:
                if type(paragraph) == str:
                    list_of_paragraphs_to_ret.append(paragraph)
                else:
                    if paragraph._VALUE != None:
                        list_of_paragraphs_to_ret.append(paragraph._VALUE)
    return list_of_paragraphs_to_ret

def get_paragraphs_list_from_body(spark: SparkSession, input_dir):
    df = spark.read \
                .format('com.databricks.spark.xml') \
                .options(rowTag='record') \
                .options(rowTag='body') \
                .load(input_dir)
    
    return  df.select("p") \
            .rdd \
            .map(lambda row: parse_paragraphs_list(row['p'])) \
            .zipWithIndex() \
            .map(lambda record: (record[1], record[0])) \

def parse_sections_list(list_of_sections):
    if list_of_sections != None:
        list_of_sections = [parse_paragraphs_list(section.p) for section in list_of_sections if section.p != None]
    else:
        list_of_sections = []
    return list_of_sections

def get_sections_list(spark: SparkSession, input_dir):
    df = spark.read \
                .format('com.databricks.spark.xml') \
                .options(rowTag='record') \
                .options(rowTag='body') \
                .load(input_dir)
    
    return  df.select("sec") \
            .rdd \
            .map(lambda row: parse_sections_list(row['sec']), ) \
            .zipWithIndex() \
            .map(lambda record: (record[1], record[0]))

def build_body(row_sec, row_p):
    concat_parsed_sections = ' '.join([' '.join(section) for section in parse_sections_list(row_sec)])
    concat_standalone_paragraphs = ' '.join(parse_paragraphs_list(row_p))
    
    return concat_standalone_paragraphs + " " + concat_parsed_sections

def get_bodys_list(spark: SparkSession, input_dir):
    df = spark.read \
                .format('com.databricks.spark.xml') \
                .options(rowTag='record') \
                .options(rowTag='body') \
                .load(input_dir)
    return  df.select("sec", "p") \
            .rdd \
            .map(lambda row: build_body(row['sec'], row["p"])) \
            .zipWithIndex() \
            .map(lambda record: (record[1], record[0]))

def get_abstract_list(spark: SparkSession, input_dir):
    df = spark.read \
            .format('com.databricks.spark.xml') \
            .options(rowTag='record') \
            .options(rowTag='abstract') \
            .load(input_dir)
    return  df.select("sec", "p") \
            .rdd \
            .map(lambda row: build_body(row['sec'], row["p"])) \
            .zipWithIndex() \
            .map(lambda record: (record[1], record[0]))

def get_article_categories(row):
    categories = []
    try: 
        for subj_group in row['subj-group']:
            if type(subj_group['subject']) == str:
                categories = [subj_group['subject']]
            elif type(subj_group['subject']) == list:
                for cat in subj_group['subject']:
                    categories.append(cat)
    except:
        return [row['subj-group']['subject']]
    return row['subj-group']

def get_categories_list(spark: SparkSession, input_dir):
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

def get_titles_list(spark: SparkSession, input_dir):
    df = spark.read \
            .format('com.databricks.spark.xml') \
            .options(rowTag='record') \
            .options(rowTag='metadata') \
            .options(rowTag='article') \
            .options(rowTag='front') \
            .options(rowTag='article-meta') \
            .options(rowTag='title-group') \
            .load(input_dir)
    
    return  df.select("article-title") \
            .rdd \
            .map(lambda row: row['article-title']) \
            .zipWithIndex() \
            .map(lambda record: (record[1], record[0]))

def get_final_rdd(spark: SparkSession, input_dir):
    return get_abstract_list(spark, input_dir) \
                .join(get_bodys_list(spark, input_dir)) \
                .join(get_categories_list(spark, input_dir)) \
                .join(get_titles_list(spark, input_dir)) \
                .sortByKey()

def get_final_to_df(spark: SparkSession, input_dir):
    return get_final_rdd(spark, input_dir) \
                .map(lambda record: (record[1][0][0][0], record[1][0][0][1], record[1][0][1], record[1][1])) \
                .toDF(["abstract", "body", "categories", "title"]) \
                .withColumn("id", monotonically_increasing_id())    