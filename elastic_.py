import sys
import os
import pandas as pd 
import numpy as np 
from elasticsearch.connection import RequestsHttpConnection
from elasticsearch import helpers, Elasticsearch
from elasticsearch_dsl import Search
import csv

def check_if_indices_exists(es,index_name):
	if es.indices.exists(index_name):
		print "Index exists"
		return True
	else:
		print "Index doesnt exists"
		return False

def create_index(es,index_name):
	if check_if_indices_exists(es,index_name):
		print "Index already exists"
	else:
		print "Creating index"
		es.indices.create(index_name)

def create_index_alias(es, index_name, alias_name):
    if check_if_indices_exists(es, index_name):
        # if not es.indices.exists_alias(index_name, alias_name):
        alias_status = es.indices.put_alias(index=index_name, name=alias_name, body={
            "filter": {
                "match": {
                    "TId": alias_name
                }
            }
        })
        print "alias created", alias_status
    else:
        print "index doesn't exist, not creating alias"


def check_index_alias_exists(es, index_name, alias_name):
    if es.indices.exists_alias(index_name, alias_name):
        return True
    else:
        return False




def delete_index(es, index_name):
    if check_if_indices_exists(es, index_name):
        es.indices.delete(index_name, ignore=[400, 404])
        print "index deleted"
    else:
        print "index doesn't exists"


def upsert_generic_doc_to_es(es, index_name, doc_type_name, file_path, identifier_col):
    if check_if_indices_exists(es, index_name):
        print "index already exists, need to perform upserts"
    else:
        print "index doesn't exists"
    with open(file_path) as f:
        reader = csv.DictReader(f)
        actions = ({
            "_index": index_name,
            "_type": doc_type_name,
            "_id": f[identifier_col],
            "doc": f,
            "_op_type": 'update',
            'doc_as_upsert': True
            } for f in reader
                   )
        print helpers.bulk(es, actions)




if __name__ == '__main__':
	data_dir= '../data/'
	print os.listdir(data_dir)
	data_= os.listdir(data_dir)
	server="localhost:9200"

	es_server = Elasticsearch(server, connection_class=RequestsHttpConnection)

	print "Ping elasticsearch:",es_server.ping()
	data_to_consider='bc_data.csv'
	print "Lets look at the data:"
	data= pd.read_csv(data_dir+ data_to_consider)
	print data.head()

	print "Understanding the hierarchy, index_name>data:"

	print "Check if index exists:"

	index_name="data_index"

	# delete_index(es_server,index_name)

	print "creating index:",index_name

	create_index(es_server,index_name)

	print "Creating index alias:"

	create_index_alias(es_server,index_name,"breast_cancer")

	print "Inserting document"

	# upsert_generic_doc_to_es(es_server,index_name,"breast_cancer_doc",data_dir+ data_to_consider,"Id")









