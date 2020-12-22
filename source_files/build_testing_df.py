import findspark
import os
import warnings
import random

from shutil import copyfile
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession


findspark.init('/usr/local/Cellar/apache-spark/3.0.1/libexec')

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

warnings.simplefilter("ignore")


root_dir = "../datasets/PubMed - medium samples/2016/"

project_path = os.path.abspath('..')

no_of_files = 5
destination = project_path + "/datasets/2016_testing_df/"
source = project_path + "/datasets/2016_all_data/"

if '2016_testing_df' not in os.listdir("../datasets/"):
    os.mkdir("../datasets/2016_testing_df/")
    
    f = open("../datasets/2016_testing_df/.gitkeep", "w")
    f.close()

    all_file_names = os.listdir(source)
    for current_choice_idx in range(no_of_files):
        current_file = random.choice(all_file_names)
        while current_file in os.listdir(destination) and current_file != '.gitkeep':
            current_file = random.choice(all_file_names)
        print("Current_file_chosen " + current_file)
        copyfile(source + current_file, destination + current_file)
        print("\tCopied " + current_file + " to /datasets/2016_testing_df/")

    print("Finished gathering data for the testing df")
else:
    print("You already have a testing df in place.")