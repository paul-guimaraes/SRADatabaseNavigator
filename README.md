<!-- TODO: escrever manual -->
# SRA Database Navigator
SRA Database Navigator is a computational [Python](https://www.python.org/) package
designed to explore the metadata of sequenced samples stored in the [Sequence Read Archive](https://www.ncbi.nlm.nih.gov/sra)
(SRA). This package helps you locate and group samples sharing the same characteristic by comparing both structured
and unstructured metadata annotated in the SRA. The package constructs relational databases in the
[PostgreSQL](https://www.google.com/url?sa=t&source=web&rct=j&opi=89978449&url=https://www.postgresql.org/&ved=2ahUKEwiwp7GPjIKKAxXFrJUCHX12CxkQFnoECA4QAQ&usg=AOvVaw0He1mmeTUi_lhXjiRGJtzr)
Database Management System (DBMS).
You can integrate Python modules into your project or use the scripts to download SRA metadata, construct local
databases, normalize sample metadata, and integrate and filter samples into networks.
For detailed methodology, please consult the article.<!-- TODO: inserir link para o artigo. -->

## 📋 Dependencies
Python 3.10 or higher and python lybraries described into [requirements.txt](./requirements.txt) file.

## 🔧 Instalação
The simplest way to install the project's dependencies is through the [Pip](https://pypi.org/project/pip/) module. 
On a device with Python3 installed, execute the command below to install all the necessary dependencies.
```shell
pip install -r requirements.txt 
```
If you wish to keep the installation isolated in a virtual environment, you can use the venv module to create the 
environment, activate it, and install the dependencies separately. Replace <virtual_environment_name> with the name of
your virtual environment. If you prefer, you can perform the same operations using your [Conda](https://anaconda.org/anaconda/conda)
environment or in your preferred IDE.
```shell
# creating virtual environment.
python -m venv <virtual_environment_name>
# loading virtual environment.
source <virtual_environment_name>/bin/activate
# intall dependencies.
pip install -r requirements.txt 
```
It is common for both Python versions 2 and 3 to coexist on many systems. If version 3 is not the default version on 
your system, replace the commands 'python' and 'pip' with 'python3' and 'pip3', respectively.

All the processes below are executed using the full path to the location where the SRADatabaseNavigator were installed.
If frequent execution is required, you may add the database directory to the PATH in your environment variables.
Please refer to your operating system's documentation for instructions on how to perform this operation. By doing so, 
you will be able to execute all scripts by simply typing their names in the terminal. For example: `sra.py --help`.

## 🚀 Usage
The execution of this package involves three main steps: downloading the data from the SRA, indexing the data, and
constructing feature networks of the samples.
### 💾 Download data
The location and transfer of the SRA data, as well as the construction of the database, are carried out using the script
[sra.py](./database/sra.py), located in the directory [database/data/sra.py](database/sra.py).
To construct the database, you can execute the command "sra.py --database <database_name> --query <SRA Query in Entrez Standard>".
```shell
# Use NCBI Entrez standard.
# Querying NCBI SRA and store data into local database. 
<path to SRADatabaseNavigator>/database/sra.py --database my_happy_database --password --query '(acute lymphoblastic leukemia) AND "Mus musculus"[orgn:__txid10090]'
# Querying NCBI SRA and store data into database in another machine.
<path to SRADatabaseNavigator>/database/sra.py --host <server address> --port <server port> --user <server user> --database <server database name> --password --query <Entrez SRA query>
```
Consult the documentation using the --help option.
```shell
# Visualizando opções de comando no script. 
<path to SRADatabaseNavigator>/database/sra.py --help
```
### 🔍 Data indexing
Create a configuration file into [database](database) directory named "config.ini" with the database connection details. An example file is available at
[config.ini.example](database/config.ini.example).
```ini
; Config model for packages.
[database]
host=localhost
port=5432
user=user
name=database_name
password=top_secret
```

To index the data in the local database, execute the script [prepare_database.py](database/prepare_database.py).
```shell
<path to SRADatabaseNavigator>/database/prepare_database.py
```
### Network process
After indexing the local database, you can use the query interface to customize the system's functionality and query the
data.
#### Web tool
No primeiro acesso configure as sessões do serviço com o script [manage.py](database/web/manage.py)
```shell
<path to SRADatabaseNavigator>/database/web/manage.py migrate
```
You can use the script [manage.py](database/web/manage.py) to start the local web server. On a local machine, run one of the following commands:
```shell
# To run the web server only on a local machine, use the following command:
<path to SRADatabaseNavigator>/database/web/manage.py runserver

# To run the web server on a specific address, use the following command:
<path to SRADatabaseNavigator>/database/web/manage.py runserver <address>:<port>

# To run the web server into all local network, use the following command:
<path to SRADatabaseNavigator>/database/web/manage.py runserver 0.0.0.0:<port>
```

[!WARNING]
Never use the manage.py script directly to serve the application, as this may expose security vulnerabilities on your system.
Instead, use programs such as [Apache HTTP Server](https://httpd.apache.org/) ou outro de sua preferência.

Should you wish to make the tool available via web access, you can do so using web servers such as Apache HTTP Server.
To configure and integrate the web tool into your Apache server, you may include the directives below. 
Please refer to the documentation of [Apache HTTP Server](https://httpd.apache.org/) and the [Django Framework](https://www.djangoproject.com/) for further details.
```
WSGIScriptAlias / /path/to/mysite.com/mysite/wsgi.py
WSGIPythonHome /path/to/venv
WSGIPythonPath /path/to/mysite.com

<Directory /path/to/mysite.com/mysite>
<Files wsgi.py>
Require all granted
</Files>
</Directory>
```
##### Running the tool
Then, access the chosen address in an updated web browser. For example: http://127.0.0.1:8000/
<br/>![SRA Navigator start screen](readme_files/sra_navigator_start.png)<br/>
##### Solving ambiguities
Due to the type of data, the indexing process may generate ambiguities, and therefore some terms may not be resolved 
automatically. In such cases, you should access the "Join Fields" menu and assess the detected ambiguities. If you agree
with the join, click the "Accept" button; otherwise, click the "Reject" button.
<br/>![SRA Navigator Join Fields Option](readme_files/sra_navigator_join_fields.png)<br/>
##### Querying database
Click on the "Filter" menu to search data in the integrated database. The target table is the table that stores the 
data you are searching for. The "Selected table filters" section displays the filters applied to the search. Filters 
are table attributes. When the goal is to use all the fields of the table, you can select the table itself instead of 
each individual field. Right-click on the attribute or table you want to select in the filter, then click on the "Add" 
option to include it in the search, or "Remove" to exclude it from the filter. It is also possible to remove fields and
tables from the search by clicking on the element in the "Selected table filters" section.
Click the "All fields" button to select all fields from all tables. The "Clear" button removes all previously selected fields.

<br/><img alt="SRA Navigator filter data" src="readme_files/sra_navigator_filter.png" width="500"><br/>

When your filter is complete, click the "Get network" button to start the search.

<br/><img alt="SRA Navigator filter get network" src="readme_files/sra_navigator_filter_get_network.png" width="500"><br/>

Once the processing is complete, you will receive a summary of the results and will be able to download them for analysis on your computer.

<br/><img alt="SRA Navigator filter result 1" src="readme_files/sra_navigator_filter_result_1.png" width="500"><br/>
<br/><img alt="SRA Navigator filter result 2" src="readme_files/sra_navigator_filter_result_2.png" width="300"><br/>

The result is a set of files containing an index file (index.html) to be opened in a web browser, the file used to 
construct the network ("input_graph.csv"), as well as accessory data in CSV and JSON formats. The "query.json" file 
contains the details of the search performed and the time spent on execution.

<br><img alt="SRA Navigator filter result 3" src="readme_files/sra_navigator_filter_result_3.png" width="500"><img alt="SRA Navigator filter result 4" src="readme_files/sra_navigator_filter_result_4.png" width="500"><br/>

If you need to customize the construction of the network, hide labels, use parallel processing, or even achieve 
performance improvements in the network construction process, you can use the script [network.py](database/network.py).
Please, consult the `--help` option to see all the parameters.
```shell
<path to SRADatabaseNavigator>/database/network.py --input /tmp/teste/jobs/input_graph.csv --work_directory /tmp/rede/
```
##### Extracting elements from the results
Given the nature of the search, analyzing or extracting data from networks can be a complex task. To assist with these 
analyses, we provide the check_community.py[check_community.py](database/check_community.py) and 
the [check_community_by_csv_terms.py](database/check_community_by_csv_terms.py) scripts. The scripts processes
the data from each network, generates summaries, and extracts features to be analyzed in a combined manner.
To use them, copy the networks generated in CSV format (typically the files named "network_community_%d.csv" with the 
header "label, node_a, node_b, weight") into a directory and execute the command.
```shell
# Gerando resumo e extraíndo detalhes sobre arestas e nós da rede.
<path to SRADatabaseNavigator>/database/check_community.py --input_directory <path to network csv files> --output_directory <path to output directory> --summarize --extract_fields
```
For each community, the script check_community.py[check_community.py](database/check_community.py) will generate a 
summary containing the ID, the number of nodes, the number of connections, attributes, and values that constitute the edges.

Additionally, a directory will be created with the name of each community, containing lists of attribute names, edge 
labels, terms (features), and values found per sample, as well as the edge weights in the network, sorted from lowest 
to highest. The weight is a dynamic value calculated based on the edge and associated with the number of features that 
form the edge. The smaller the weight, the fewer the number of features the edge represents.

````shell
# Focusing on edge details and seeking combinations.
<path to SRADatabaseNavigator>/database/check_community_by_csv_terms.py --input <path to network terms.csv file created by check_community.py script> --output <path to output directory>
````
The script [check_community_by_csv_terms.py](database/check_community_by_csv_terms.py) uses the term.csv file generated
by the [check_community.py](database/check_community.py) script. After retrieving the terms, the script performs a 
series of combination tests, starting with pairs (2 by 2) and increasing up to n by n, with n representing the maximum
number of elements that can be grouped together. For each combination, a CSV file is generated displaying the details of
the grouping. When possible, a Venn diagram is included.

If you wish to extract the types of sequencing strategies present in your network, use the 
[separate_by_strategy.py](database/separate_by_strategy.py) script.
```shell
<path to SRADatabaseNavigator>/database/separate_by_strategy.py --work_directory <path to network directory generated by check_community.py with present_values.csv>
```

To reorder the summary by the columns 'Connections', 'Attributes', and 'Values', use the [sort_indexes.py](database/sort_indexes.py) script.
```shell
<path to SRADatabaseNavigator>/database/sort_indexes.py --index_file <path to summary.csv file>
```

###### Static plots
To display the results, we adapted the [Pyvis](https://pypi.org/project/pyvis/) library to visualize the networks 
dynamically. However, networks composed of many elements may place a heavy load on the computer during the visualization
process. For this reason, we provide the [plot_networkx.py](database/plot_networkx.py) script, which generates static 
figures with a lower computational cost using the [NetworkX](https://networkx.org/) library.

```shell
<path to SRADatabaseNavigator>/database/plot_networkx.py --input <path to csv network like 'network_community_0.csv'> --output <output directory>
```

##### Filtering networks by edge weights
The networks can be filtered based on the edge weights. To perform this procedure, you can use the 
[network.py](database/network.py) script in combination with some commands in a Linux shell environment.
```shell
# Storing the file name in the variable FILE_NAME.
FILE_NAME='network_community_0.csv'
NAME_WITHOUT_ENTENSION=$(basename -s .csv $FILE_NAME)

# Extracting edge weights from the network with quantities.
sed 's/\r//g' $FILE_NAME | rev | cut -f1 -d',' | rev | sort | uniq -c > weights.txt

# Generating a filtered file with all the weights detected in the file weights.txt.
for i in $(grep -v 'weight' weights.txt | rev | cut -f1 -d' ' | rev); do head -1 ${FILE_NAME} | sed 's/\r//g' > ${NAME_WITHOUT_ENTENSION}_filtered_${i}.csv; sed 's/\r//g' ${FILE_NAME} | grep ${i}'$' >> ${NAME_WITHOUT_ENTENSION}_filtered_${i}.csv; done

# Building a network for each filtered file generated.
for file in $(ls *filtered*csv | sort -r); do echo Workin on ${file}...; time <path to SRADatabaseNavigator>/database/network.py --debug --thread --is_graph_file --prefix network_ --work_directory $(basename -s .csv $file) --input $file; done
```

## Citation
Guimarães, P.A.S., Carvalho, M.G.R. & Ruiz, J.C. A computational framework for extracting biological insights from SRA cancer data. Sci Rep 15, 8117 (2025). <a href="https://doi.org/10.1038/s41598-025-91781-8" target="_blank">doi.org/10.1038/s41598-025-91781-8</a>
