import os
from datetime import timedelta
from datetime import datetime
import datetime
from os.path import isfile, join
import warcio
from multiprocessing import Pool
from functools import partial
import json
import pytz
import sys



  

# =============================================================================
# ssh = SSHClient()
# ssh.load_system_host_keys()
# ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# ssh.connect('geotwitter2.uncg.edu',username='s_kachap', password='testgeotwitter')
# 
# 
# print('Checking for json folders in given Path:')
# stdin,stdout,stderr = ssh.exec_command("ls  *.json /hdd/sfm-processing/export ")
# #print(stdout.readlines())
# dir_path = os.path.dirname(os.path.realpath(__file__))
# print(dir_path)https://gwu-libraries.github.io/sfm-ui/
#https://gwu-libraries.github.io/sfm-ui/
#https://github.com/koaning/human-learn

    
# =============================================================================



#find_Tweets_By_Date(onlyfiles,start_date,end_date)               
#print(len(data))

warcData = {}
data = []
counter = 0


def FindPoint(x1, y1, x2,  
              y2, x, y) : 
    if (float(x) > float(x1) and float(x) < float(x2) and 
        float(y) > float(y1) and float(y) < float(y2)) : 
        return True
    else : 
        return False

def co_values(a,inputData):
    x1 = inputData[2]
    y1 = inputData[3]
    x2 = inputData[4]
    y2 = inputData[5]
    if a['place'] !=None:
        co_values = a['place']['bounding_box']['coordinates']
                    #print(co_values)
                    #value_Exist = check(co_values[0][0][0],co_values[0][0][1],co_values[0][1][0],co_values[0][1][1],co_values[0][2][0],co_values[0][2][1],co_values[0][3][0],co_values[0][3][1],x,y)
        #value_Exist = FindPoint(co_values[0][0][0],co_values[0][0][1],co_values[0][2][0],co_values[0][2][1],x,y)
        if(FindPoint(x1,y1,x2,y2,co_values[0][0][0],co_values[0][0][1])and FindPoint(x1,y1,x2,y2,co_values[0][1][0],co_values[0][1][1])and FindPoint(x1,y1,x2,y2,co_values[0][2][0],co_values[0][2][1])
           and FindPoint(x1,y1,x2,y2,co_values[0][3][0],co_values[0][3][1])):
                #print(value_Exist)
          return True
        else:
          return False 
          



def is_json(myjson):
  try:
    json.loads(myjson)
  except ValueError as e:
    return False
  return True

def find_Tweets_By_Coordinates(inputData, filename):
    check_start_date = inputData[0]
    check_end_date = inputData[1]
    print(filename)
    global counter
    with open(filename, 'rb') as stream:
        for record in warcio.ArchiveIterator(stream):
            if record.rec_type == 'response':
                print(record.rec_headers.get_header('WARC-Target-URI'))
                print(record.rec_headers.get_header('Content-Length'))
               # warcData = record.content_stream().read().decode('utf-8')
                print('Reading tweets from', filename)
                sys.stdout.flush()
                for line in record.content_stream().read().decode('utf-8').split('\n'):
                    if len(line.strip()) != 0 and is_json(line) :
                            a = json.loads(line)
                            counter = counter + 1
                            if ('created_at' in a ):
                                formattedDate = a['created_at'].replace('+0000','')
                                date = datetime.datetime.strptime(formattedDate, '%a %b %d %H:%M:%S %Y')
                                if((date > check_start_date and date < check_end_date) and co_values(a, inputData)) and 'extended_entities' in a and a['extended_entities']['media'][0]['media_url']!=None:
                                    data.append(a)
                                    print(a['extended_entities']['media'][0]['media_url'])
                                    sys.stdout.flush()
                return data  
        
final_list = []
def retList(results):
    outFile = open("finalResult.json", "a")
    for sublist in results:
        if sublist is not None:
            for item in sublist:
                if item is not None:
                    final_list.append(item)
                    json.dump(item, outFile)
                    
                    #print(len(final_list))
    return final_list


if __name__ == '__main__':
    year = int(input('Enter the year (Year should be greater than 2018: )'))
    end_year = int(input('Enter the end year (Year should be greater than 2018: )'))
    start_month = int (input('Enter the first month in number: '))
    end_month = int (input('Enter the second month in number: '))
    #date = input('Enter the date in number')
    start_date = int(input('Enter the start_date in number: '))
    end_date = int(input('Enter the end_date in number: '))
    hour = int(input('Enter start_time in hour hh:'))
    minute = int(input('Enter start_time in minute mm:'))
    second = int(input('Enter start_time in second ss:'))
    end_hour = int(input('Enter end_time in hour hh:'))
    end_minute = int(input('Enter end_time in minute mm:'))
    end_second = int(input('Enter end_time in second ss:'))
    x1 = input('Enter the value of x1 latitude: ')
    y1 = input('Enter the value of y1 longitude: ')
    x2 = input('Enter the value of x2 latitude: ')
    y2 = input('Enter the value of y2 longitude: ')


    startDateForJson = datetime.datetime(year, start_month, start_date, hour, minute, second)
    
    endDateForJson = datetime.datetime(end_year, end_month, end_date, end_hour, end_minute, end_second)
   



  
    
    
   
filelist = []

dateCounter = int (start_date)
monthCounter = int (start_month)
startMonth = int (start_month)

loopCounter = 0

if (dateCounter <= int(end_date) and monthCounter == end_month and year == end_year ):
    print("Months are equal")
    while (dateCounter <= int(end_date)):
        path =  "/hdd/sfm-data/collection_set/aacdb5e2af86455f9061d75da214d484/3514b35ff05242098763dc69da57a525/" + str(year) +"/" + str(start_month) +"/" + str(dateCounter)
        print('Reading in path', path)
        sys.stdout.flush()
        for root, dirs, files in os.walk(path):
            for file in files:
                print(file)
                sys.stdout.flush()
                filelist.append(os.path.join(root,file))
        dateCounter = dateCounter + 1

elif (monthCounter != end_month):
    print("Months are not equal")
    print (start_month)
    print(end_month)
    while(year <= end_year):
        if(year == end_year):
            endMonth = end_month
        else:
            endMonth = 12
        while(startMonth <= endMonth): 
            if (loopCounter!=0):
                    startDate = 1;
            else:
                startDate = int(start_date)
            if (startMonth == endMonth and year == end_year):
                endDate = int(end_date)
            else:
                 endDate = 31;
            print(startDate)
            print(endDate)
            while(startDate <= endDate):
                path =  "/hdd/sfm-data/collection_set/aacdb5e2af86455f9061d75da214d484/3514b35ff05242098763dc69da57a525/" + str(year) +"/" + str(startMonth) +"/" + str(startDate)
                print(path)
                startDate = startDate + 1;
                for root, dirs, files in os.walk(path):
                    for file in files:
                        print(file)
                        filelist.append(os.path.join(root, file))
                        #print(len(filelist))
                    
            startMonth = startMonth + 1
            loopCounter = loopCounter + 1
        year = year + 1
        startMonth = 1
        

       
#print all the file names
#2018for name in filelist:
    #print(name)
print(len(filelist))
print(startDateForJson)
print(endDateForJson)


start_time = datetime.datetime.now()

inputData=[]
inputData.append(startDateForJson)
inputData.append(endDateForJson)
inputData.append(x1)
inputData.append(y1)
inputData.append(x2)
inputData.append(y2)
    
func = partial(find_Tweets_By_Coordinates, inputData)
pool = Pool(10)
results = pool.map(func, filelist)
#print(results)
a = retList(results)

       
    
print('Done')
#find_Tweets_By_Coordinates(onlyfiles,start_date,end_date,x,y)
#print(len(data2))
end_time = datetime.datetime.now()
print('Duration: {}'.format(end_time - start_time))







    

