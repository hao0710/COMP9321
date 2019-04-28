'''
COMP9321 Assignment One Code Template 2019T1
Name:
Student ID:
'''
#questions can not run respectively, please run whole codes in order to get correct answer.
# I submited the original map I use to color, run whole file to see the coloured map 
import pandas as pd
import csv
import json
import numpy as np
import matplotlib.pyplot as plt
from PIL import Image
df_accidents=pd.read_csv('accidents_2017.csv',encoding='utf-8',float_precision='high')
df_2=pd.DataFrame(data=df_accidents)# prepare a dataframe for future use, after run q2 the df_2 contains dropped all Unknown and "-" rows, but keep duplicated data.
df_3=pd.DataFrame(data=df_accidents)#prepare a dataframe for future use, after run q3 the df_3 will not contain duplicated data.

#double quote if space between a string
def double_quote_1(a):
    if type(a) == str and ' ' in a:
        return '"'+a.strip()+'"'   
    else:
        return a

#convert string to title style remove extra spaces at the end of each string
def convert_name(a):
    if type(a)==str:
        a=a.split()
        for i in range(len(a)):
            if a[i]!="de" and  a[i]!="la" and not a[i].startswith("d'") and not a[i].startswith("l'") and not a[i].startswith('2017'):
                a[i]=a[i].title()
        return ' '.join(a)            
    return a
#print data in table rows, separated by one single space as required
def print_frame(dataframe):
    col_names = dataframe.columns
    for c in range(len(col_names)):
        if c==len(col_names)-1:
            print(double_quote_1(col_names[c]))
        else:
            print(double_quote_1(col_names[c]), end=' ')
    for index, row in dataframe.iterrows():
        for cur_name in col_names:               
            if cur_name==col_names[-1]:
                print(row[cur_name])
            else:
                print(double_quote_1(row[cur_name]), end =' ')
# remove invalid data from dataframe, return valid dataframe 
def remove_invalid(dataframe):
    col_names=dataframe.columns
    for index, row in dataframe.iterrows():
        for col in col_names:
            if str(row[col])=='Unknown' or str(row[col])=='unknown' or str(row[col])=='-' or str(row[col])=='--'or str(row[col])=='na' or str(row[col])=='NA' \
            or str(row[col])=='Na' or str(row[col])=='nan' or str(row[col])=='Nan' or str(row[col])=='NAN' or str(row[col])=='NaN' or str(row[col])=='NAn':                
                dataframe=dataframe.drop(index)
                break
    return dataframe
    

def q1():
    df_1=pd.DataFrame(data=df_accidents.head(10)).applymap(lambda x: convert_name(str(x).strip()))
    print_frame(df_1)
#q1()

def q2(): 
    global df_2
    df_2=remove_invalid(df_2).applymap(lambda x: convert_name(x)) 
    df_2.to_csv('result_q2.csv',quotechar='"',index=False,quoting= csv.QUOTE_NONNUMERIC)
    pass 
#q2()

def q3():
    global df_3   
    df_3=df_2.drop_duplicates(subset='Id')# df2 contains duplicated data, drup duplicated data first
    # build a new dataframe result to store the desired output
    result = pd.DataFrame(df_3['District Name'].value_counts().reset_index().values, columns=["District Name", "Total numbers of accidents"])
    #result_colnames=result.columns

    print_frame(result)  
    '''
    Put Your Question 3's code in this function 
    '''
    pass 
#q3()
def convert_hour(column_data):
    return str(column_data[0:-3])

def convert_month(column_data):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July','August', 'September', 'October', 'November', 'December']
    return str(month_list[int(column_data)-1])

def convert_date(column_data):
    return str(int(column_data))
#remove air quality contatins invalid data.
def remove_air_quality(dataframe):
    col='Air Quality'
    temp=[]
    for index, row in dataframe.iterrows():
        if str(row[col])=='Unknown' or str(row[col])=='unknown' or str(row[col])=='-' or str(row[col])=='--'or str(row[col])=='na' or str(row[col])=='NA' or str(row[col])=='Na' or str(row[col])=='nan' or str(row[col])=='Nan' or str(row[col])=='NAN' or str(row[col])=='NaN' or str(row[col])=='NAn':
            temp.append(index)
    dataframe=dataframe.drop(temp)
    return dataframe 

def q4():
    air_station=pd.read_csv('air_stations_Nov2017.csv',encoding='utf-8',float_precision='high')
    air_quality=pd.read_csv('air_quality_Nov2017.csv',encoding='utf-8',float_precision='high')
    df_air_station=pd.DataFrame(data=air_station)
    df_air_quality=pd.DataFrame(data=air_quality)

    df_air_quality=remove_air_quality(df_air_quality).applymap(convert_name) # I am not sure whether we should remove NA in other colums. If remove all the invalid data please comment this line and run next line
    #df_air_quality=remove_invalid(df_air_quality).applymap(convert_name) 

    df_air_station=remove_invalid(df_air_station).applymap(convert_name)
    df_air_quality['O3 Hour'] = df_air_quality['O3 Hour'].apply(lambda x: str(x).lower())#convert back to the lower case h
    df_air_quality['NO2 Hour']=df_air_quality['NO2 Hour'].apply(lambda x: str(x).lower())
    df_air_quality['PM10 Hour']=df_air_quality['PM10 Hour'].apply(lambda x: str(x).lower())

    df_air_station_json=pd.DataFrame(df_air_station[['Station',"District Name"]])
    print(df_air_station_json.to_json(orient='records'))#q4_1

    #df_air_quality_copy=df_air_quality.copy()
    df_air_quality_not_good=df_air_quality.copy()
    tmp=[]   
    for index,row in df_air_quality_not_good.iterrows():
        if row['Air Quality']=='Good':
            tmp.append(index)
    df_air_quality_not_good=df_air_quality_not_good.drop(tmp)
    print_frame(df_air_quality_not_good.head(10)) #q4_2
    #copy three dataframe for future use
    df_air_quality_not_good_copy=df_air_quality_not_good.copy()
    df_air_station_copy=df_air_station_json.copy()# df_air_station_json only contains station and district name.
    df_air_quality_not_good_copy=df_air_quality_not_good_copy[['Station','Air Quality','Generated']]


    #split the Generated attribute and convert to proper type. Hour like 12:00 convert to 12, Month convert to English version
    #date like 01 in original csv convert to 1
    df_air_quality_not_good_copy[['date','month','year']]=df_air_quality_not_good_copy['Generated'].str.split('/',expand=True)
    df_air_quality_not_good_copy[['Tmp','hour']]=df_air_quality_not_good_copy['year'].str.split(' ',expand=True)
    df_air_quality_not_good_copy=df_air_quality_not_good_copy.drop(columns=['Generated','year','Tmp'])
    df_air_quality_not_good_copy['hour']=df_air_quality_not_good_copy['hour'].astype(str).apply(convert_hour)
    df_air_quality_not_good_copy['month']=df_air_quality_not_good_copy['month'].astype(str).apply(convert_month)
    df_air_quality_not_good_copy['date']=df_air_quality_not_good_copy['date'].astype(str).apply(convert_date)

    
    global df_3
    valid_accidents=df_3.copy()

    #create quality with district name
    df_quality_district=pd.merge(df_air_quality_not_good_copy,df_air_station_copy,on='Station')# hence the air_quality data would contain district name

    df_q4=pd.merge(valid_accidents,df_quality_district,on='District Name')
    #df_q4.to_csv('df_q4.csv')
    tmp=[]
    for index, row in df_q4.iterrows():
        if str(row['date'])!=str(row['Day']) or str(row['Month'])!=str(row['month']) or str(row['Hour'])!=str(row['hour']):
            tmp.append(index)
    result=df_q4.drop(tmp)
    result=result.drop(columns=['Station','Air Quality','date','month','hour'])
    result=result.drop_duplicates()
    result=result.sort_values(['Id'])
    result.to_csv('result_q4.csv',quotechar='"',index=False,quoting= csv.QUOTE_NONNUMERIC)

    '''
    Put Your Question 4's code in this function 
    '''
    pass 
#q4()
def q5():
    fig=plt.gcf()
    im = plt.imread('Map.png')
    implot = plt.imshow(im)
    global df_3
    y=df_3['Latitude'].tolist()
    x=df_3['Longitude'].tolist()
    a=map(lambda x : (float(x)-2.006874)/((2.334-2.006874)/964),x)
    b=map(lambda x : (float(x)-41.462289)/((41.314292-41.462289)/576),y)
    plt.scatter(list(a),list(b),s=0.001,c='r')
    plt.xticks([])
    plt.yticks([])
    ax = plt.axes()
    ax.yaxis.set_major_locator(plt.NullLocator())
    ax.xaxis.set_major_formatter(plt.NullFormatter())
    plt.box(on=False)
    plt.axis(alpha=0,visible=False)
    plt.show()
    plt.show()
    fig.savefig('Map.png')

    pass 


q1()
q2()
q3()

















