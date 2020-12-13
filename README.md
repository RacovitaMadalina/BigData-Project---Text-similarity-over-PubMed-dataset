# BigData Project - Detecting Text Similarities in PubMed dataset

![cover](https://www.google.com/url?sa=i&url=https%3A%2F%2Fwww.cshl.edu%2Flatest-advances-malaria-research-free-ebook-cold-spring-harbor-laboratory-press%2F&psig=AOvVaw22pcURp1Wi1l12Vk37NLqB&ust=1607962492833000&source=images&cd=vfe&ved=0CAIQjRxqFwoTCNjgoJyty-0CFQAAAAAdAAAAABAD)

Project developed in the first semester of the second year of the master studies in Computational Optimization,
Faculty of Computer Science, Iasi, for the BigData laboratory.

<br></br>

### Authors:
- Dinu Sergiu Andrei
- Vintur Cristian
- Strugari Stefan
- Racovita Madalina-Alina

<br></br>

### Initial project description

**Statistics for sub-domain specific publications** (e.g. Malaria research) on **structural properties of the text** such as (but not only!) 
- number of _formulas_
- number of _figures_
- number of _experiments_
- number of _authors_
- _author affiliation (name & location)_
- number of _citations_
- number of _conclusions_
- number of _sections/subsections/paragraphs_
- _paper length (number of words, number of pages)_
- _average paragraph length_, etc.

**Applications**:
- Statistics on structural properties of publications for each sub-field (Malaria, Cancer, etc.)
- Being able to classify publications and/or “guess” the research sub-domain based on their structural properties
- Domain-specific stats (e.g. how many figures do papers related to Malaria have)

<br></br>

### Updated project description

In this project we are going to provide an **Exploratory Data Analysis** for the PubMed dataset. 
We are going to provide **statistics for the above mentioned criterias**.

Furthermore, we are going to exceed a **text similarity detection** for these articles. **The algorithms** 
used for the text similarity (Min-Hashing, K-means) are going to be **compared** later on **in a performance analysis section**.

<br></br>

### Project development plan 

- **Phase 1**: **Environment configuration**, Github repository 
- **Phase 2**: **Parsing the dataset**, all archives, gathering all files together
- **Phase 3**: Since the `small samples` dataset is not available anymore, we are going to **build
our own testing dataset**, with a smaller size comparing to the initial one
- **Phase 4**: **Provide statistics** / visualization and insights about the data
- **Phase 5**: **Research** on the algorithms used for text similarity detection on datasets of large dimensions (ideas: Min-hashing)
- **Phase 6**: Discuss and transform the initial dataset into an **established form for training the algorithms** (
decide if the algorithms are going to be fit on texts given in XML schemas or should we parse those texts before training?)
- **Phase 7**: **Min-hashing implementation**: return most similar pairs of articles and their similarity percentage
- **Phase 8**: **Cluster** the texts using **K-means**
- **Phase 9**: **Inside each cluster** returned by K-means **apply a text similarity algorithm** (like TF-IDF or Gensim or Spacy)
are return most similar pairs of articles and their similarity percentage
- **Phase 10**: **Performance results comparison**: compare K-means with Min-Hashing
- **Phase 11**: Configure the project to run inside **Google Cloud**, using Spark jobs inside a cluster

<br></br>

### References

**Link to the dataset**: https://drive.google.com/drive/folders/0B6LHYB5SN9DEWWdXQUNkS3NVOW8