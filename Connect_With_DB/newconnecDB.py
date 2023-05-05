import pandas as pd
from sqlalchemy import create_engine,text,sql
import time
from urllib.request import urlopen


engine = create_engine('mysql+mysqlconnector://root@127.0.0.1:3306/r_nytt_main')
query =text("SELECT * FROM factory")
with engine.connect() as conn:
    # smtp = conn.execute(query)
    table = pd.read_sql(query,conn)
    # print(table)

list_of_facid= table[(table['isDeleted'] == '0')]['factoryId'].tolist()
# print(df)   

sorce_eng = create_engine('mysql+mysqlconnector://root@127.0.0.1:3306/r_nytt_factory1')
# print(sorce_eng)
query = text("SELECT * FROM machine")
with sorce_eng.connect() as con:
    sou_table = pd.read_sql(query,con)
    # print(sou_table)

list_of_machinname = (sou_table[(sou_table['isDeleted'] == '0')]['machineName'].unique())
# print(list_of_machinname)

sorce_eng1 = create_engine('mysql+mysqlconnector://root@127.0.0.1:3306/r_nytt_factory5')
query = text("SELECT * FROM machine")
with sorce_eng1.connect() as connn:
    sou_table1 = pd.read_sql(query,connn)
    # print(sou_table1)
list_of_machin_name = sou_table1[(sou_table1['isDeleted'] == '0')]['machineName'].unique()
# print(list_of_machin_name)

print("r_nytt_factory",list_of_facid[0])
print()
for machine_name in list_of_machin_name:
    print(machine_name)
print()


print("r_nytt_factory",list_of_facid[1])
print()
for machinename in list_of_machinname:
    print(machinename)   

# a = [machin for machin in list_of_machinname]
# print(a)







