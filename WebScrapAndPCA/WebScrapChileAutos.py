from bs4 import BeautifulSoup
import requests
import numpy as np 
import time
import re
from pymongo import MongoClient
from multiprocessing import Pool
import pandas as pd


def extractHeader(x):
    dictionary_header = {"Marca":x.get('data-webm-make'),
                         "Model":x.get('data-webm-model'),
                         "Price":x.get('data-webm-price'),
                         "ID":x.get('id'),
                         "Region":x.get('data-webm-state'),
                         "Type_use":x.get('data-webm-vehcategory')}
    return dictionary_header

def extractImages(images):
    dict_images = {}
    for count,item in enumerate(images[0].find_all('img')):
        if(isinstance(item.get('data-src'),str)):
            dict_images[f"image_{count}"] = item.get('data-src')
        else:
            dict_images[f"image_{count}"] = item.get('src')
    #clean images
    return dict_images

def extractKeyDetails(x,title,resumen):
    #clean Variational Input
    resumen["Title"]=title.replace('\n','').strip()
    
    for i in range(len(x)):
        dato = x[i].text
        data_type = x[i].get('data-type')
        if(data_type =="Odometer"):
            dato = dato.replace('.','')
            dato = dato.replace(' km','')
            resumen["Km"]=dato.strip()
        elif(data_type =="Transmission"):
            resumen["Transmission"]=dato
        elif(data_type =="Fuel Type"):
            resumen["Fuel Type"]=dato
        else:
            resumen["Fuel Economy"]=dato
    return resumen


def SolicitarPage(URL,time_sleep):
    #proxy = calcularProxies()
    #for i in range(time_sleep):
    #    time.sleep(1)
    #    print("....",end=".")
    print("\n............. Procesando Query .....................\n")
    try:
        #print(f"proxies restantes: {len(proxy)}")
        #random choice y eliminar ese mismo elemento de la lsita
        #proxy_actual  = np.random.choice(proxy)
        #print(proxy_actual)
        #pxy = {'http':proxy_actual,'https':proxy_actual}
        headers = requests.utils.default_headers()
        headers.update({'User-Agent':'Mozilla/5.0 (X11; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',})
        response = requests.get(URL,headers=headers,timeout=5)
        #response = requests.get(URL,proxies=pxy,timeout=5)
        #print(f"Status de la consulta: {response.status_code}\n")
        return BeautifulSoup(response.text,"html5lib")
    except:
        print("Fallo response...\n")
        return -1
    

def getTotalIter(soup):
    total_vehiculos = soup.h1.text.replace('\n','').strip()
    total_cars = re.sub(r'\.|[a-z]|[A-Z]|\,','',total_vehiculos)
    total_car = int(total_cars.split()[0])
    if(total_car%12!=0):
        iterations = int(total_car/12)+1
    else:
        iterations = int(total_car/12)
    return iterations

def calcularProxies():
    df_proxies = pd.read_csv("proxy-list-master/proxy-list.txt",sep=" ")
    df_proxies = df_proxies[df_proxies["status"]=="+"]
    df_proxies.reset_index(inplace=True,drop=True)
    df_proxies.drop("empty",axis=1,inplace=True)
    df_proxies = df_proxies[df_proxies["status"]=='+']
    return df_proxies['ip'].values.tolist()

def extraerBrand(list_autos):
    return_marcas = []
    for item in list_autos:
        #return_marcas.append("https://www.chileautos.cl/vehiculos/"+str(item.split('/')[-2:-1][0]))
        return_marcas.append(str(item.split('/')[-2:-1][0]))
    return return_marcas

def saveDB(soup,db,k):
    objects_count=0
    all_divs = soup.find_all(class_='listing-items')
    #test_all_divs = all_divs[0].find_all('div',class_="listing-item card standard")
    #test_all_autos = all_divs[0].find_all('div',class_="listing-item card showcase")
    if(k>=4):
        all_autos = all_divs[0].find_all('div',class_="listing-item card standard")
        if(len(all_autos)==0):
            all_autos = all_divs[0].find_all('div',class_="listing-item card showcase")
        #print(f"Cantidad de datos en la pagina: {len(all_autos)}")
    else:
        #top_auto = all_divs[0].find_all('div',class_="listing-item card topspot")
        all_autos = all_divs[0].find_all('div',class_="listing-item card showcase")
        if(len(all_autos)==0):
            all_autos = all_divs[0].find_all('div',class_="listing-item card standard")
        #print(f"Cantidad de datos en la pagina: {len(all_autos)}")
    if(len(all_autos)!=0):
        for i, head in enumerate(all_autos):
            objects_count+=1
            result_header = extractHeader(head)
            result_header["Images"] = extractImages(head.find_all("div",class_="card-header"))
            card_body = head.find_all("div",class_="card-body")
            temp = card_body[0].find_all('ul',class_="key-details")
            key_details = extractKeyDetails(temp[0].find_all('li'),
                                            card_body[0].h3.text,
                                            result_header)
            collection = db.vehiculos
            insert=collection.insert_one(key_details).inserted_id

    #print(f"{objects_count} objects Stored...\nDone...\n")

def getQuery(indice,brand,rango):
    try:
        URL=f"https://www.chileautos.cl/vehiculos/?q=(And.Marca.{brand.capitalize()}._.Ano.range({rango[0]}..{rango[1]}).)&offset={12*indice}"
        #print(URL,"\n")
        soup = SolicitarPage(URL,int(13*np.random.rand()))
        if(soup!=-1):
            total_iter = getTotalIter(soup)
        else:
            soup = -1
            print("Error, saliendo del programa....")
    except:
        print(f"Error en query: marca={brand}, rango=({rango[0]},{rango[1]}), indice={12*indice}\n")
    return soup, total_iter
    
def extraerDatos(brand,rang,iterations,db):
    print("Preparando la extraccion de datos.....................")
    for k in range(1,iterations):
        try:
            soup,itera = getQuery(k,brand,rang)
            saveDB(soup,db,k)
        except:
            print(f"Error en marca: {brand}, range: {rang},iteraciones: {k}\n")

            
def launchTask(brands,db):
    """scale_year = [[1901,2000],
                  [2001,2006],
                  [2007,2010],
                  [2011,2013],
                  [2014,2015],
                  [2016,2017],
                  [2018,2019],
                  [2019,2020],
                  [2021,2022]]"""
    scale_year = [[2018,2019],
                 [2019,2020],
                  [2021,2022]]
    #p=Pool(6)
    for marca in brands:
        for rang in scale_year:
            if(marca != "chevrolet" and marca != "toyota"):
                try:
                    _,total_iter = getQuery(1,marca,rang) #2da pagina y total de iter necesarias
                    print(f"extract para---->  Marca: {marca}, Rango:({rang[0]},{rang[1]}) =  {total_iter}")
                    #p.map(partial(extraerDatos,
            #                         brand=marca,
            #                         iterations=total_iter,
            #                         db=db),
                    #               scale_year)
                    extraerDatos(marca,rang,total_iter,db)
                    print(f"Passed {marca} {rang}")
                except:
                    print("Error de programa")

def main():
    URL="https://www.chileautos.cl/vehiculos"
    soup = SolicitarPage(URL,5)
    #iterations = getTotalIter(soup)
    href_autos = []
    for link in soup.find_all('a'):
        href_autos.append(link.get('href'))
    print("Start Application\n")
    #print(f"total de vehiculos: {iterations*12}")
    href_autos=href_autos[69:75]
    brand_cars = extraerBrand(href_autos)
    print(brand_cars)
    #brand_cars =["mercedez"]
    #print(brand_cars)
    launchTask(brand_cars,db)

if __name__=="__main__":
    client = MongoClient('localhost',27017)
    db = client['AutosChilecl']
    datos_test = main()