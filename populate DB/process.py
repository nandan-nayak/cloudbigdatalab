import pandas as pd
#import pymssql
import pyodbc

inputfile=pd.read_csv("DataSet.csv");

#conn=pymssql.connect(host=r'DESKTOP-0MFRJ4E', user=r'root', password=r'root', database=r'bigdata')

inputfile=pd.read_csv("DataSet.csv");

#conn=pymssql.connect(host=r'DESKTOP-0MFRJ4E', user=r'root', password=r'root', database=r'bigdata')


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-0MFRJ4E;'
                      'Database=bigdata;'
                      'Trusted_Connection=yes;')


cur = conn.cursor()

def preprocess(columnname,index):
    #print(type(inputfile.iloc[index][columnname]))
    if list(inputfile.columns).index(columnname)!=0:
        if str(inputfile.iloc[index][columnname]).lower() in ["nan","inf"]:
            return preprocess(columnname,index-1)
        else:
            return float(inputfile.iloc[index][columnname])
    else:
        return inputfile.iloc[index][columnname]


    
for i in range(len(inputfile)):
    #print(preprocess("Dynamic Power",i))
    cur.execute('insert into product values(?,?,?,?,?,?)',
                preprocess("Model",i),
                preprocess("Delay",i),
                preprocess('Dynamic Power',i),
                preprocess('Static Power',i),
                preprocess('TotalPower',i),
                preprocess('Power-DelayProduct',i))
                
    
#row = cur.fetchone()
#print(row[0])
    
    
print("executed successfully")
cur.commit();
cur.close()
conn.close()


