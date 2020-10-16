# -*- coding: utf-8 -*-
"""
Created on Tue Aug 18 18:19:19 2020

@author: samhitha
"""
 
from os import listdir
from os.path import isfile, join
from datetime import datetime
import datetime
from multiprocessing import Pool
from functools import partial
import json


def area(x1, y1, x2, y2, x3, y3): 
      
    return abs((float(x1) * (float(y2) - float(y3)) + 
               float(x2) * (float(y3) - float(y1)) + 
               float(x3) * (float(y1) - float(y2))) / 2.0) 
  
def check(x1, y1, x2, y2, x3,  
          y3, x4, y4, x, y): 
                
    # Calculate area of rectangle ABCD  
    A = (area(x1, y1, x2, y2, x3, y3) +
         area(x1, y1, x4, y4, x3, y3)) 
  
    # Calculate area of triangle PAB  
    A1 = area(x, y, x1, y1, x2, y2) 
  
    # Calculate area of triangle PBC  
    A2 = area(x, y, x2, y2, x3, y3) 
  
    # Calculate area of triangle PCD  
    A3 = area(x, y, x3, y3, x4, y4) 
  
    # Calculate area of triangle PAD  
    A4 = area(x, y, x1, y1, x4, y4); 
  
    # Check if sum of A1, A2, A3  
    # and A4 is same as A  
    return (A == A1 + A2 + A3 + A4)

def findPoint(x1, y1, x3,  
              y3, x, y) : 
    if (float(x) > float(x1) and float(x) < float(x3) and 
        float(y) > float(y1) and float(y) < float(y3)) : 
        return True
    else : 
        return False


def find_Tweets_By_Date(onlyfiles, start_date, end_date ):
 for filename in onlyfiles:
      if ".json" in filename:
          with open("/hdd/sfm-processing/export" + filename) as f:
            for line in f:
                a = json.loads(line)
                date = datetime.datetime.strptime(a['created_at'], '%a %b %d %H:%M:%S %z %Y')
                
                if(date > start_date and date < end_date):
                    print(line)
                    data.append(json.loads(line))
                else:
                    break

def find_Tweets_By_Coordinates(inputData, filename):
    start_date = inputData[0]
    end_date = inputData[1]
    x = inputData[2]
    y = inputData[3]
    data = []

    if ".json" in filename:
          with open("/hdd/sfm-processing/export" + filename) as f:
            print ('Reading ', filename)
            for line in f:
                a = json.loads(line)
                date = datetime.datetime.strptime(a['created_at'], '%a %b %d %H:%M:%S %z %Y')
                
                if(date > start_date and date < end_date):
                    #print(line)
                    data.append(json.loads(line))
                #print(a)
                if a['place'] !=None:
                    co_values = a['place']['bounding_box']['coordinates']
                    #print(co_values)
                    value_Exist = findPoint(co_values[0][0][0],co_values[0][0][1],co_values[0][1][0],co_values[0][1][1],co_values[0][2][0],co_values[0][2][1],co_values[0][3][0],co_values[0][3][1],x,y)
                    #print(value_Exist)
                    if value_Exist == True:
                        if a['user']['profile_image_url'] != None:
                             data.append(a)
                             #print(len(data))
            return data



final_list = []
def retList(results):
     outFile = open("data.json", "a")
    for sublist in results:
        if sublist is not None:
            for item in sublist:
                if item is not None:
                    final_list.append(item)
                    json.dump(item, outfile)
                    #print(len(final_list))
    return final_list
                              




#find_Tweets_By_Date(onlyfiles,start_date,end_date)               
#print(len(data))

if __name__ == '__main__':
    onlyfiles = [f for f in listdir("/hdd/sfm-processing/export") 
    if isfile(join("/hdd/sfm-processing/export/", f))]
    #print(onlyfiles)
    data = []
    start_date_entry = input('Enter a start_date in YYYY-MM-DD format:  ')
    start_time_entry = input('Enter start_time in hh:mm:ss: ')
    year, month, day = map(int, start_date_entry.split('-'))
    hour, minute, second = map(int,start_time_entry.split(':'))
    start_date = datetime.datetime(year, month, day, hour, minute, second, tzinfo=datetime.timezone.utc)


    end_date_entry = input('Enter a end_date in YYYY-MM-DD format: ')
    end_time_entry = input('Enter end_time in hh:mm:ss: ')
    year, month, day = map(int, end_date_entry.split('-'))
    hour, minute, second = map(int,end_time_entry.split(':'))
    end_date = datetime.datetime(year, month, day, hour, minute, second, tzinfo=datetime.timezone.utc)

    print('Retrieving tweets between:', start_date, '&', end_date)
    x = input('Enter the value of x')
    y = input('Enter the value of y')
    start_time = datetime.datetime.now()

    inputData=[]
    inputData.append(start_date)
    inputData.append(end_date)
    inputData.append(x)
    inputData.append(y)
    
    func = partial(find_Tweets_By_Coordinates, inputData)
    pool = Pool(10)
    results = pool.map(func, onlyfiles)
    print(results)
    print('Done')
    #find_Tweets_By_Coordinates(onlyfiles,start_date,end_date,x,y)
    #print(len(data2))
    end_time = datetime.datetime.now()
    print('Duration: {}'.format(end_time - start_time))


 



    

