from sqlalchemy import create_engine
import pandas as pd
import time
from urllib.request import urlopen
import json
# import numpy as


def readTableFromDb(engine, tableName=None, queryString=None):
    ''' Read Table in database and return pandas dataframe '''
    try:
        if queryString:
           
            with engine.connect() as con:
                table = pd.read_sql(queryString, con)
                return table

        if tableName:
            
            table = pd.read_sql_table(tableName, engine.connect())
            return table
    except Exception as e:
        return 'notExist'
    return None



def Makeconnection_factory(database):
    ''' For Connection With Database'''
    engine = create_engine(f'mysql+mysqlconnector://root:@127.0.0.1:3306/{database}')
    return engine


def Factory_connection_with_db():
    ''' For Connection With Factory in Database Which Is Not Deleted'''
    factory_connection = Makeconnection_factory('r_nytt_main')
    factory_Table= readTableFromDb(factory_connection, tableName='factory')
    List_of_factoryId = factory_Table[(factory_Table['isDeleted'] == '0')]['factoryId'].to_list()
    return List_of_factoryId

def Machine_name_in_machines():
    ''' For Connect with Seprate Factory Id of Machines Which Is Not Deleted And Get Machine Names'''
    factorydb = create_engine(f'mysql+mysqlconnector://root:@127.0.0.1:3306/r_nytt_factory{factoryid}')
    machine_table = readTableFromDb(factorydb,tableName='machine')
    machine_name = machine_table[(machine_table['isDeleted'] == '0')]['machineName'].unique()
    return machine_name


for factoryid in Factory_connection_with_db():
    print(f"r_nytt_factory{factoryid}")

    for machinename in Machine_name_in_machines():
        print(f"    {machinename}")

