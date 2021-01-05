import findspark

from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


# findspark.init('/usr/local/Cellar/apache-spark/3.0.1/libexec')
# 
# conf = SparkConf().setMaster("local").setAppName("My App")
# sc = SparkContext(conf=conf)
# spark = SparkSession(sc)


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
                .options(rowTag='record')\
                .options(rowTag='body')\
                .load(input_dir)
    return df.select("p")\
             .rdd\
             .map(lambda row: parse_paragraphs_list(row['p']))\
             .zipWithIndex()\
             .map(lambda record: (record[1], record[0]))


def parse_sections_list(list_of_sections):
    if list_of_sections != None:
        list_of_sections = [parse_paragraphs_list(section.p) for section in list_of_sections if section.p != None]
    else:
        list_of_sections = []
    return list_of_sections


def get_sections_list(spark: SparkSession, input_dir):
    df = spark.read \
                .format('com.databricks.spark.xml') \
                .options(rowTag='record')\
                .options(rowTag='body')\
                .load(input_dir)
    return  df.select("sec")\
              .rdd\
              .map(lambda row: parse_sections_list(row['sec']), )\
              .zipWithIndex()\
              .map(lambda record: (record[1], record[0]))


def build_body(row_sec, row_p):
    concat_parsed_sections = ' '.join([' '.join(section) for section in parse_sections_list(row_sec)])
    concat_standalone_paragraphs = ' '.join(parse_paragraphs_list(row_p))
    return concat_standalone_paragraphs + " " + concat_parsed_sections


def get_bodys_list(spark: SparkSession, input_dir):
    df = spark.read \
                .format('com.databricks.spark.xml') \
                .options(rowTag='record')\
                .options(rowTag='body')\
                .load(input_dir)
    return df.select("sec", "p")\
            .rdd\
            .map(lambda row: build_body(row['sec'], row["p"]))\
            .zipWithIndex()\
            .map(lambda record: (record[1], record[0]))


def get_abstract_list(spark: SparkSession, input_dir):
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

    return df.select("article-title") \
        .rdd \
        .map(lambda row: row['article-title']) \
        .zipWithIndex() \
        .map(lambda record: (record[1], record[0]))


def get_page_counts(row):
    try:
        return str(row['page-count']._count)
    except:
        return "Not specified"


def get_number_of_pages_list(spark: SparkSession, input_dir):
    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='record') \
        .options(rowTag='metadata') \
        .options(rowTag='article') \
        .load(input_dir)

    try:
        return df.select("counts") \
            .rdd \
            .map(lambda row: get_page_counts(row['counts'])) \
            .zipWithIndex() \
            .map(lambda record: (record[1], record[0]))
    except:
        return df.select("_xmlns") \
            .rdd \
            .map(lambda row: "Not specified") \
            .zipWithIndex() \
            .map(lambda record: (record[1], record[0]))


def get_authors(row):
    authors = []
    try:
        for contrib in row['contrib']:
            print(contrib)
            if contrib['_contrib-type'] == 'author':
                authors.append(contrib['name']['given-names'] + ' ' + contrib['name']['surname'])
        return authors
    except:
        return ['Not specified']


def get_authors_list(spark: SparkSession, input_dir):
    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='record') \
        .options(rowTag='metadata') \
        .options(rowTag='article') \
        .options(rowTag='front') \
        .options(rowTag='article-meta') \
        .load(input_dir)

    return df.select("contrib-group") \
        .rdd \
        .map(lambda row: get_authors(row['contrib-group'])) \
        .zipWithIndex() \
        .map(lambda record: (record[1], record[0]))


def get_affiliations(row):
    affiliations = dict()
    affiliations_for_authors = []
    try:
        for aff in row['aff']:
            affiliations[aff['_id']] = (aff['country'], aff['institution'])

        for contrib in row['contrib']:
            if contrib['_contrib-type'] == 'author':
                ids_aff = [aff['_rid'] for aff in contrib['xref']]
                affiliations_for_authors.append([])

                for id_aff in ids_aff:
                    affiliations_for_authors[-1].append(affiliations[id_aff])
        return affiliations_for_authors
    except:
        return ['Not specified']


def get_affiliations_list(spark: SparkSession, input_dir):
    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='record') \
        .options(rowTag='metadata') \
        .options(rowTag='article') \
        .options(rowTag='front') \
        .options(rowTag='article-meta') \
        .load(input_dir)

    return df.select("contrib-group") \
        .rdd \
        .map(lambda row: get_affiliations(row['contrib-group'])) \
        .zipWithIndex() \
        .map(lambda record: (record[1], record[0]))


def get_fig_no(row):
    try:
        if row['fig'] != None:
            if type(row['fig']) == list:
                return str(len(row['fig']))
            return "1"
        else:
            return 'Not specified'
    except:
        return 'Not specified'


def get_number_of_figures(spark: SparkSession, input_dir):
    df = spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='record') \
        .load(input_dir)

    return df.select('fig') \
        .rdd \
        .map(lambda row: get_fig_no(row)) \
        .zipWithIndex() \
        .map(lambda record: (record[1], record[0]))


def get_simplified_rdd(spark: SparkSession, input_dir):
    return get_abstract_list(spark, input_dir) \
                .join(get_bodys_list(spark, input_dir)) \
                .join(get_categories_list(spark, input_dir)) \
                .join(get_titles_list(spark, input_dir)) \
                .sortByKey()


def get_simplified_to_df(spark: SparkSession, input_dir):
    return get_simplified_rdd(spark, input_dir) \
                .map(lambda record: (record[1][0][0][0], record[1][0][0][1], record[1][0][1], record[1][1])) \
                .toDF(["abstract", "body", "categories", "title"]) \
                .withColumn("id", monotonically_increasing_id())


def get_final_rdd(spark: SparkSession, input_dir):
    return get_abstract_list(spark, input_dir) \
        .join(get_paragraphs_list_from_body(spark, input_dir)) \
        .join(get_sections_list(spark, input_dir)) \
        .join(get_bodys_list(spark, input_dir)) \
        .join(get_categories_list(spark, input_dir)) \
        .join(get_titles_list(spark, input_dir)) \
        .join(get_number_of_pages_list(spark, input_dir)) \
        .join(get_authors_list(spark, input_dir)) \
        .join(get_affiliations_list(spark, input_dir)) \
        .join(get_number_of_figures(spark, input_dir)) \
        .sortByKey()


def get_final_to_df(spark: SparkSession, input_dir):
    return get_final_rdd(spark, input_dir) \
                .map(lambda record: (record[1][0][0][0][0][0][0][0][0][0],
                                     record[1][0][0][0][0][0][0][0][0][1],
                                     record[1][0][0][0][0][0][0][0][1],
                                     record[1][0][0][0][0][0][0][1],
                                     record[1][0][0][0][0][0][1],
                                     record[1][0][0][0][0][1],
                                     record[1][0][0][0][1],
                                     record[1][0][0][1],
                                     record[1][0][1],
                                     record[1][1]))\
                .toDF(["abstract", "paragraphs", "sections", "body", "categories", "title", "pages count",
                       "authors", "affiliations", "figures count"]) \
                .withColumn("id", monotonically_increasing_id())
