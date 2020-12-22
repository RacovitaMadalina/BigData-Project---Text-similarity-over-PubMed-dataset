import os
import warnings
import findspark

from distutils.dir_util import copy_tree
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession


findspark.init('/usr/local/Cellar/apache-spark/3.0.1/libexec')

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

warnings.simplefilter("ignore")

root_dir = "../datasets/PubMed - medium samples/2016/"

if '2016_all_data' not in os.listdir("../datasets/"):
    os.mkdir("../datasets/2016_all_data/")

f = open("../datasets/2016_all_data/.gitkeep", "w")
f.close()

project_path = os.path.abspath('..')

destination = project_path + "/datasets/2016_all_data/"
source = project_path + "/datasets/PubMed - medium samples/2016/"


def modify_content_current_dir(path_dir):
    if '__COMPLETED__' in os.listdir(path_dir):
        os.remove(path_dir + '__COMPLETED__')
        print("____DELETED __COMPLETED__ file from current dir. ")
        
    day = path_dir.split('/')[-2:-1][0].split('_')[0]
    
    for file in os.listdir(path_dir):
        if '.DS_Store' == file:
            os.remove(path_dir + '.DS_Store')
        if '2016' not in file:
            os.rename(path_dir + file, path_dir + day + "_" + file)
            
    print("____RENAMED all files to contain " + day + " in their file naming.")


if '.DS_Store' in os.listdir(source):
    os.remove(source + '.DS_Store')


for file in sorted(os.listdir(source)):
    abs_path_current = source + file + "/"
    print("Current directory: " + file)
    if '2016' in abs_path_current:
        modify_content_current_dir(abs_path_current)
        copy_tree(abs_path_current, destination)
        print('Copied all the files from current directory to </datasets/2016_all_data/>.\n')




