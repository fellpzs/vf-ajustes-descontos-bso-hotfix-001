#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import os
import time

import psycopg2
import requests
import datetime
import simplejson as json
from psycopg2 import Error
from psycopg2.extras import RealDictCursor

from jrvsecrets import get_secret

boticario_instances = ['botivf-temp']

endpoint = '.cluster-ro-cgqnm2keuwx7.us-east-1.rds.amazonaws.com'
remove_databases = ["template0", "template1", "rdsadmin", "varejofacil", "postgres", "varejofacil_template"]
jrv_url = 'https://jarvis.casamagalhaes.services/api/v1'

DB_LOCAL = 'cp10011'
DB_PORT_LOCAL = '5432'
DB_USER_LOCAL = 'postgres'
DB_PASS_LOCAL = 'postgres'

class ContainerResult:
    result = []
    def __init__(self, name):
        self.name = name

def get_container(container):
    data = {}

    try:
        response = requests.get(f'{jrv_url}/admin/containers',
            params={'active': 'true', 'name': container},
            headers={'x-api-key': get_secret('BIGHOSTS_KEY')}
        )

        if response.status_code != 200:
            raise Exception('Não foi possível consultar a API, code: ', response.status_code)

        if len(json.loads(response.text).get('items')) == 0:
            raise Exception('container não identificado')

        items = json.loads(response.text).get('items')[0]
        data['id'] = items['id']
        data['container'] = items['name']
        data['clusterId'] = items['clusterId']

        return data
    except Exception as e:
        print(e)
        raise

def get_properties(containerId):
  properties = {}

  try:
    response = requests.get(f'{jrv_url}/admin/containers/{containerId}/properties',
      headers={'x-api-key': get_secret('BIGHOSTS_KEY')}
    )

    if response.status_code != 200:
      raise

    properties['username'] = json.loads(response.text).get('syspdvweb.jdbc.username')
    properties['password'] = json.loads(response.text).get('syspdvweb.jdbc.password')

    return properties
  except:
    print('não foi possível obter as propriedades do container')
    raise

def close_connection(db, conn):
    if (db):
        conn.close()
        db.close

def query_to_json_file(environment, query):
    
    result = execute_query(environment, query, None)

    resultStr = json.dumps(result)
    # Using a JSON string
    with open('query_result.json', 'w') as outfile:
        outfile.write(resultStr)

def execute_query_in_db(dbConfigAndQuery):
    new_db = psycopg2.connect(
                host=dbConfigAndQuery["host"],
                port=dbConfigAndQuery["port"],
                database=dbConfigAndQuery["database"],
                user=dbConfigAndQuery["user"],
                password=dbConfigAndQuery["password"]
            )
    new_connection = new_db.cursor(cursor_factory=RealDictCursor)
    new_connection.execute(dbConfigAndQuery["query"])

    records = [r for r in new_connection.fetchall()]

    close_connection(new_db, new_connection)

    return records
    

def json_default(value):
    if isinstance(value, datetime.datetime):
        return value.__str__()
    elif isinstance(value, datetime.date):
        return value.__str__()
    else:
        return value.__dict__

def execute_query(query, database=''):
    
    container_data = get_container(database)
    props = get_properties(container_data['id'])

    new_db = psycopg2.connect(
            host=container_data['clusterId']+endpoint,
            port="3306",
            database=container_data['container'],
            user=props['username'],
            password=props['password']
        )
    new_connection = new_db.cursor(cursor_factory=RealDictCursor)
    new_connection.execute(query)

    records = [r for r in new_connection.fetchall()]

    close_connection(new_db, new_connection)

    containerResult = ContainerResult(database)
    containerResult.result = records
    
    return containerResult


def execute_update(environment, query, database=''):
    result = []

    
    container_data = get_container(database)
    props = get_properties(container_data['id'])

    new_db = psycopg2.connect(
            host=container_data['clusterId']+endpoint,
            port="3306",
            database=container_data['container'],
            user=props['username'],
            password=props['password']
        )
    with new_db.cursor() as new_connection:
        try:
            new_connection.execute(query)

            records = new_connection.statusmessage
            new_db.commit()
        except Exception as e:
            print("Erro: ")
            print(e)
        finally: 
            close_connection(new_db, new_connection)

        containerResult = ContainerResult(database)
        containerResult.result = records
        result.append(containerResult)
    
    return result

def execute_update_local(query, database=DB_LOCAL):
    result = []
    list_instances = ['localhost']
    portNum = DB_PORT_LOCAL
    databases = [database]
    userName = DB_USER_LOCAL
    pass_environment = DB_PASS_LOCAL
    
    for dbName in databases:
        
        new_db = psycopg2.connect(
                host=list_instances[0],
                port=portNum,
                database=dbName,
                user=userName,
                password=pass_environment
            )
        
        with new_db.cursor() as new_connection:
            try:
                new_connection.execute(query)
                
                records = new_connection.statusmessage
                new_db.commit()
            except Exception as e:
                print("Erro: ")
                print(e)
            finally: 
                close_connection(new_db, new_connection)
        
            containerResult = ContainerResult(dbName)
            containerResult.result = records
            result.append(containerResult)
        
    return result

def execute_queries_in_container(environment, queries, containerName):
    result = []
    
    if environment == 'boticario':
        list_instances = boticario_instances
        pass_environment = get_secret('BOTICARIO_PWD_SALT')
    elif environment == "local":
        return execute_queries_in_container_local(environment, queries, containerName)

    database="varejofacil"
    user="varejofacil"
    password=pass_environment

    for clusterId in list_instances:
           
        db = psycopg2.connect(
                host=clusterId+endpoint,
                port="3306",
                database=database,
                user=user,
                password=password
        )
        connection = db.cursor()
        connection.execute("select datname from pg_database where datname = '{}' order by 1".format(containerName))

        close_connection(db, connection)

        container_data = get_container(containerName)
        props = get_properties(container_data['id'])
        queriesResult = []
        print("--> Executando em: {}".format(containerName))
        with psycopg2.connect(
                host=container_data['clusterId']+endpoint,
                port="3306",
                database=container_data['container'],
                user=props['username'],
                password=props['password']
            ) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as curs:
                try:
                    for q in queries:
                        print('   ', q)
                        curs.execute(q)
                        records = curs.rowcount
                        print("    Num. Linhas: {}".format(records))
                        queriesResult.append(records)

                finally: 
                    close_connection(conn, curs)

                containerResult = ContainerResult(containerName)
                containerResult.result = queriesResult
                for row in containerResult.result:
                    row['base'] = containerName
                result.append(containerResult)
    
    return json.dumps(result, default = json_default)

def execute_queries_in_container_local(environment, queries, containerName):
    result = []
    queriesResult = []
    
    port = DB_PORT_LOCAL
    database=containerName
    user=DB_USER_LOCAL
    password=DB_PASS_LOCAL
    print("--> Executando em: {}".format(database))
    with psycopg2.connect(
            host="localhost",
            port=port,
            database=database,
            user=user,
            password=password
        ) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as curs:
            try:
                for q in queries:
                    print('   ', q)
                    curs.execute(q)
                    if(curs.rowcount > 0):
                        records = [r for r in curs.fetchall()]
                    print("    Num. Linhas: {}".format(len(records)))
                    queriesResult = records

            finally: 
                close_connection(conn, curs)

            containerResult = ContainerResult(containerName)            
            for row in queriesResult:
                base = {"base" : containerName}
                base.update(row)
                containerResult.result.append(base)
            result.append(containerResult)
    
    return json.dumps(result[0].result, default = json_default)

def execute_update_in_container(queries, containerName, container_data, props):
    result = []
    
    queriesResult = []
    print("--> Executando em: {}".format(containerName))
    with psycopg2.connect(
            host=container_data['clusterId']+endpoint,
            port="3306",
            database=container_data['container'],
            user=props['username'],
            password=props['password']
        ) as conn:
        with conn.cursor() as curs:
            try:
                allQueries = ''.join(queries)
                print('   ', allQueries)
                curs.execute(allQueries)
                records = curs.rowcount
                print("    Num. Linhas: {}".format(records))
                queriesResult.append(records)
            except Exception as e:
                print("Erro: ")
                print(e)
            finally: 
                close_connection(conn, curs)

            containerResult = ContainerResult(containerName)
            containerResult.result = queriesResult
            result.append(containerResult)
    
    return json.dumps(result, default = json_default)

