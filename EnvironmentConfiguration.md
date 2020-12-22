## Environment configuration

<br></br>

### How to add Spark XML to your Pyspark

Download this jar: https://mvnrepository.com/artifact/com.databricks/spark-xml_2.12/0.5.0

Add the jar to `$SPARK_HOME/jars` directory. 

Example on MacOS: `SPARK_HOME=/usr/local/Cellar/apache-spark/3.0.1/libexec`


<br></br>

### Download data from OneDrive

The PubMed data is put under gitignore, so you need firstly to download yourself locally.

**Link to the dataset**: https://drive.google.com/drive/folders/0B6LHYB5SN9DEWWdXQUNkS3NVOW8

Download `2016.zip` archive, unzip it and put the directory `2016/` in the project under `datasets/PubMed - medium samples`.
You should have a directory structure like this:

```
datasets/
____    PubMed - medium samples/
________    2016/
____________    2016-01-01_2016-01-02/
____________    2016-01-02_2016-01-03/
____________    ...
```

<br></br>

### Python - install dependencies

Create your own virtual environment. If you're using anaconda you can do it like this:
```
conda create big-data
conda activate big-data
```

Now that you have your virtual environment set, install the dependecies:
```
pip install -r requirements.txt
```

<br></br>

### Data gathering for development

```
cd source_files/
python all_xml_files_to_single_directory.py
python build_testing_df.py
```

After you execute these commands you should have the folder structure under `datasets/` directory like this:
```
datasets/
____    2016_all_data/
____    2016_testing_df/
____    PubMed - medium samples/
________    2016/
____________    2016-01-01_2016-01-02/
____________    2016-01-02_2016-01-03/
____________    ...
```

In `2016_all_data/` directory, you'll have all the xml.gz files, from the initial dataset, gathered in a single 
directory.

In `2016_testing_df/` you have `n` random files chosen from `2016_all_data/` files. This should help us to
develop faster since, loading the entire data from `2016_all_data/` should lead to very slow development, due to 
the large amount of data, or even worse to `Out of Memory` issues.
