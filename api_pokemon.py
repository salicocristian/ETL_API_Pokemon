# -*- coding: utf-8 -*-
"""
Created on Sun May 14 10:44:33 2023

@author: CTI23643
"""

import requests
#import pandas
import redshift_connector

def get_pokemons(url='http://pokeapi.co/api/v2/pokemon-form/',offset=0 ):
    args={'offset':0, 'limit' : 2000 } 
    response=requests.get(url,params=args)
    
    if response.status_code==200:
     payload=response.json()
     results=payload.get('results',[])
     names=[]
     if results:
         
         for pokemon in results:
          #print(pokemon)
          names.append(pokemon['name'])
          
         
     
     return names
     #print(names)
if __name__=='__main__' :
 url='http://pokeapi.co/api/v2/pokemon-form/' 
 datos=get_pokemons()
 #print(datos)
conn = redshift_connector.connect(
   host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
   database='data-engineer-database',
   port=5439,
   user='salico_c_coderhouse',
   password='34Y2m5Y4Jd'
)




c = conn.cursor()
print('se conecto')
sql=' insert into pokemon(nombre) values(%s) ' 

rows = [(nombre,) for nombre in datos]  # Convertir cada nombre en una tupla individual
print(rows)
#rows=('picachu','salicaitor','Puchi') 

#c.execute(sql)
c.executemany(sql,rows)

conn.commit()

print('finalizo la insercion con exito')