# -*- coding: utf-8 -*-
import mysql.connector 
import pandas as pd
import json
import os
import streamlit as st
import plotly.graph_objs as go
import plotly.express as px
from urllib.request import urlopen
#To connect mysql database 
connector= mysql.connector.connect(
    host='localhost',
    user='root',
    password='lokesh@98',
    database="phonepe_pulse"
    )
cursor=connector.cursor()
#path to the state
path="C:/Users/GOD/Favorites/phone pay project/pulse/data/aggregated/transaction/country/india/state"
agg_user_path="C:/Users/GOD/Favorites/phone pay project/pulse/data/aggregated/user/country/india/state"
map_tran_path="C:/Users/GOD/Favorites/phone pay project/pulse/data/map/transaction/hover/country/india/state"
map_user_path="C:/Users/GOD/Favorites/phone pay project/pulse/data/map/user/hover/country/india/state"
top_tran_path="C:/Users/GOD/Favorites/phone pay project/pulse/data/data/top/transaction/country/india/state"
top_user_path="C:/Users/GOD/Favorites/phone pay project/pulse/data/data/top/user/country/india/state"
#list of states in the state directory
agg_state_list=os.listdir(path)
agg_user_state_list=os.listdir(agg_user_path)
map_tran_state_list=os.listdir(map_tran_path)
map_user_state_list=os.listdir(map_user_path)
top_tran_state_list=os.listdir(top_tran_path)
top_user_state_list=os.listdir(top_user_path)

#creating six tables in the database
#first table about aggregate_transaction
total_data={"State":[], "Year":[],"Quarter":[], "Tansaction_type":[], "Transaction_count":[], "Transaction_amount":[]}
for state in agg_state_list:
    path_year=path+'/'+state
    agg_year_list=os.listdir(path_year)
    for year in agg_year_list:
        file_path=path_year+'/'+year
        agg_file_list=os.listdir(file_path)
        for file in agg_file_list:
            data_path=file_path+'/'+file
            data=open(data_path,"r+")
            dict_data=json.load(data)
            
            for i in dict_data["data"]['transactionData']:
                trans_name=i['name']
                count=i['paymentInstruments'][0]['count'] 
                amount=i['paymentInstruments'][0]['amount']
                total_data["State"].append(state)
                total_data["Year"].append(year)
                total_data["Quarter"].append(int(file.strip(".json")))
                total_data["Tansaction_type"].append(trans_name)
                total_data["Transaction_count"].append(count)
                total_data["Transaction_amount"].append(amount)
#creating dataframe using pandas               
df_one=pd.DataFrame(total_data)
#creating table using sql query
cursor.execute("create table aggregate_transaction(state varchar(45), year int, Quarte int, tansaction_type varchar(45), transaction_count int, transaction_amount float)")
for record in df_one.values:
    cursor.execute("insert into aggregate_transaction(state, year, Quarter, tansaction_type, transaction_count, transaction_amount) values{}".format(tuple(record)))
    connector.commit()
    
#table about aggregate_users
aggregate_user={'State':[],'Year':[],'Quarter':[],'users_brand':[],'users_count':[],'user_percentage':[]}
for state in agg_user_state_list:
    path_year=agg_user_path+'/'+state
    list_year=os.listdir(path_year)
    for year in list_year:
        file_path=path_year+'/'+year
        list_file=os.listdir(file_path)
        for file in list_file:
            data_path=file_path+'/'+file
            data=open(data_path,"r+")
            dict_data=json.load(data)
            
            if dict_data['data']['usersByDevice' ] is None:
                aggregate_user['State'].append(state)
                aggregate_user['Year'].append(year)
                aggregate_user['Quarter'].append(int(file.strip(".json")))
                aggregate_user['users_brand'].append(None)
                aggregate_user['users_count'].append(None)
                aggregate_user['user_percentage'].append(None)
            else:
                for i in dict_data['data']['usersByDevice']:
                    brand=i['brand']
                    count=i['count']
                    percentage=i['percentage']
                    aggregate_user['State'].append(state)
                    aggregate_user['Year'].append(year)
                    aggregate_user['Quarter'].append(int(file.strip(".json")))
                    aggregate_user['users_brand'].append(brand)
                    aggregate_user['users_count'].append(count)
                    aggregate_user['user_percentage'].append(percentage)
                
df_two=pd.DataFrame(aggregate_user)
df_two['users_brand']=df_two['users_brand'].fillna(method='pad')
mean=df_two['users_count'].mean()
perc_mean=df_two["user_percentage"].mean()
df_two["users_count"]= df_two["users_count"].fillna(int(mean))
df_two["user_percentage"]= df_two["user_percentage"].fillna(perc_mean)
cursor.execute("create table aggregate_users(state varchar(45),year int,Quarter int,users_brand varchar(45),users_count int,user_percentage float)")
for record in df_2.values:
    cursor.execute("insert into aggregate_users(state, year, Quarter, users_brand, users_count, user_percentage) values{}".format(tuple(record)))
    connector.commit()

#third table about map_transaction
map_transaction={'State':[],'Year':[],'Quarter':[],'district_name':[],'transaction_count':[],'transaction_amount':[]}
for state in map_tran_state_list:
    year_path=map_tran_path+'/'+state
    year_list=os.listdir(year_path)
    for year in year_list:
        file_path=year_path+'/'+year
        file_list=os.listdir(file_path)
        for file in file_list:
            data_path=file_path+'/'+file
            data=open(data_path,"r+")
            dict_data=json.load(data)
            for i in dict_data['data']['hoverDataList']:
                name=i['name']
                count=i['metric'][0]['count']
                amount=i['metric'][0]['amount']
                map_transaction['State'].append(state)
                map_transaction['Year'].append(year)
                map_transaction['Quarter'].append(int(file.strip(".json")))
                map_transaction['district_name'].append(name)
                map_transaction['transaction_count'].append(count)
                map_transaction['transaction_amount'].append(amount)
                
df_three=pd.DataFrame(map_transaction)
cursor.execute("create table map_transaction(state varchar(45),year int, quarter int,district_name varchar(45),transaction_count int, transaction_amount float)")

for record in df_three.values:
    cursor.execute("insert into map_transaction(state,year,quarter,district_name,transaction_count,transaction_amount)values{}".format(tuple(record)))
    connector.commit()
    
#fourth table about map_users
map_user={'State':[],'Year':[],'Quarter':[],'district_name':[],'registered_users':[],'app_opens':[]}
for state in map_user_state_list:
    year_path=map_user_path+'/'+state
    year_list=os.listdir(year_path)
    for year in year_list:
        file_path=year_path+'/'+year
        file_list=os.listdir(file_path)
        for file in file_list:
            data_path=file_path+'/'+file
            data=open(data_path,"r+")
            dict_data=json.load(data)
    
            for i in dict_data['data']['hoverData']:
                name=i
                users=dict_data['data']['hoverData'][i]["registeredUsers"]
                appOpens=dict_data['data']['hoverData'][i]["appOpens"]
                map_user['State'].append(state)
                map_user['Year'].append(year)
                map_user['Quarter'].append(int(file.strip(".json")))
                map_user['district_name'].append(name)
                map_user['registered_users'].append(users)
                map_user['app_opens'].append(appOpens)
                
df_four=pd.DataFrame(map_user)
cursor.execute("create table map_users(state varchar(45),year int,quarter int,district_name varchar(45), registered_users int, app_opens int)")
for record in df_four.values:
    cursor.execute("insert into map_users(state, year, quarter, district_name, registered_users, app_opens)values{}".format(tuple(record)))
    connector.commit()
    
#fifth table about top_transaction
top_transaction={'State':[],'Year':[],'Quarter':[],'district_name':[] ,'total_count':[],'total_amount':[]}

for state in top_tran_state_list:
    year_path=top_tran_path+'/'+state
    year_list=os.listdir(year_path)
    for year in year_list:
        file_path=year_path+'/'+year 
        file_list=os.listdir(file_path)
        for file in file_list:
            data_path=file_path+'/'+file
            data=open(data_path,'r+')
            dict_data=json.load(data)
            for i in dict_data['data']['districts']:
                district=i['entityName']
                count=i['metric']['count']
                amount=i['metric']['amount']
                top_transaction['State'].append(state)
                top_transaction['Year'].append(year)
                top_transaction['Quarter'].append(int(file.strip(".json")))
                top_transaction['district_name'].append(district)
                top_transaction['total_count'].append(count)
                top_transaction['total_amount'].append(amount)
                
df_five=pd.DataFrame(top_transaction)
cursor.execute("create table top_transaction(state varchar(45),year int,quarter int,district_name varchar(45),transaction_count int,transaction_amount float)")
for record in df_five.values:
    cursor.execute("insert into top_transaction(state,year,quarter,district_name,transaction_count,transaction_amount)values{}".format(tuple(record)))
    connector.commit()
    
#sixth table about top_users

top_user={'State':[],'Year':[],'Quarter':[],'district_name':[],'total_users':[]}

for state in top_user_state_list:
    year_path=top_user_path+'/'+state
    year_list=os.listdir(year_path)
    for year in year_list:
        file_path=year_path+"/"+year
        file_list=os.listdir(file_path)
        for file in file_list:
            data_path=file_path+'/'+file 
            data=open(data_path,"r+")
            dict_data=json.load(data)
            for i in dict_data['data']['districts']:
                district_name=i['name']
                users=i['registeredUsers']
                top_user['State'].append(state)
                top_user['Year'].append(state)
                top_user['Quarter'].append(int(file.strip(".json")))
                top_user['district_name'].append(district_name)
                top_user['total_users'].append(users)
df_six=pd.DataFrame(top_user)
cursor.execute("create table top_users(state varchar(45),year int,quarter int,district_name varchar(45),registered_users int)")             
for record in df_six.values:
    cursor.execute("insert into top_user(state, year, quarter, district_name, registered_users) values{}".format(tuple(record)))
    connector.commit()
   
#webpage title
st.set_page_config(page_title="PhonPe_Pulse",layout="wide")
st.title("Data science Project")
#st.header(":violet[_PhonePe-Pulse_]")
new_title = '<p style="font-family:Courier; color:purple; font-size: 40px;">PhoePe-Pulse</p>'
st.markdown(new_title, unsafe_allow_html=True) 
left_column,right_column=st.columns(2)  

with left_column:
    option=st.selectbox(label="**Transactions/Users**",options=("Transaction","Users"))
    if option=='Transaction':
        transaction_data=st.selectbox(label="**Name Of Transaction**",options=("aggregate_transaction","map_transaction","top_transaction"))
        if transaction_data=='aggregate_transaction':
            type_transaction=st.selectbox(label="**Transaction Type**",options=("Recharge & bill payments","Peer-to-peer payments","Merchant payments","Financial Services","Others"))
            quarter=st.selectbox(label="**Quarter**",options=("1","2","3","4"))
            year=st.selectbox("**Year**",range(2018,2023))
            query2="select state,year,quarter,tansaction_type ,sum(transaction_amount) from {} group by state,year,quarter,tansaction_type having year={} and quarter={} and tansaction_type='{}' ".format(transaction_data,year,quarter,type_transaction)
            df=pd.read_sql_query(query2, connector)
            with urlopen('https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson')as response:
                indian_states=json.load(response)
            list_states=[]
            for i in range(len(indian_states['features'])):
                state=indian_states['features'][i]['properties']['ST_NM']
                list_states.append(state)
            st_name=sorted(list_states)
            df['st_name']=st_name
            query="select sum(transaction_amount) from aggregate_transaction"
            total_trans=pd.read_sql_query(query, connector)
            with right_column:
                st.subheader("Total Transaction value")
                st.markdown(total_trans.values)
                st.subheader("{} value".format(type_transaction))
                st.markdown(df['sum(transaction_amount)'].sum())
                st.subheader("Average {} value".format(type_transaction))
                st.markdown(df['sum(transaction_amount)'].mean())
                data =px.choropleth(df,locations='st_name',
                                    title="Total {} of {}".format(transaction_data,type_transaction),
                                    geojson=indian_states,
                                    featureidkey='properties.ST_NM',
                                    color='sum(transaction_amount)',
                                    color_continuous_scale='Reds',
                                    scope='asia',
                                    projection="mercator",
                                    hover_data=['year','quarter']
                        
                                    )
                data.update_geos(fitbounds='locations', visible=False)
                st.plotly_chart(data,use_container_width=True)
        else:
            quarter=st.selectbox(label="**Quarter**",options=("1","2","3","4"))
            year=st.selectbox("**Year**",range(2018,2023))
            query2="select state,year,quarter,sum(transaction_amount) from {} group by state,year,quarter having year={} and quarter={}".format(transaction_data,year,quarter)
            df=pd.read_sql_query(query2, connector)
            with urlopen('https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson')as response:
                indian_states=json.load(response)
            list_states=[]
            for i in range(len(indian_states['features'])):
                state=indian_states['features'][i]['properties']['ST_NM']
                list_states.append(state)
            st_name=sorted(list_states)
            df['st_name']=st_name
            with right_column:
                st.subheader("Total transaction value")
                st.markdown(df['sum(transaction_amount)'].sum())  
                st.subheader("Average transaction value")
                st.markdown(df['sum(transaction_amount)'].mean())
                data =px.choropleth(df,locations='st_name',
                                    title="Total {}".format(transaction_data),
                                    geojson=indian_states,
                                    featureidkey='properties.ST_NM',
                                    color='sum(transaction_amount)',
                                    color_continuous_scale='Reds',
                                    scope='asia',
                                    projection="mercator",
                                    hover_data=['year','quarter']
                                   
                 
                                    )
                data.update_geos(fitbounds='locations', visible=False)
                st.plotly_chart(data,use_container_width=True)
       
    elif option=='Users':
        users_data=st.selectbox(label="**Users Type**",options=("aggregate_users","map_users","top_users"))
        if users_data=="aggregate_users":
            brand=st.selectbox(label="**Brand**",options=("Xiaomi","Samsung","Vivo","Oppo","OnePlus","Realme","Apple","Motorola","Lenovo","Huawei","Others"))
            quarter=st.selectbox(label="**Quarter**",options=("1","2","3","4"))
            year=st.selectbox("**Year**",range(2018,2023))
            query2="select state,year,quarter,sum(users_count) from aggregate_users group by state,year,quarter having year={} and quarter={}".format(year,quarter)
            brand_query="select brand,sum(users_count) from aggregate_users group by brand"
            df=pd.read_sql_query(query2, connector)
            with urlopen('https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson')as response:
                indian_states=json.load(response)
            list_states=[]
            for i in range(len(indian_states['features'])):
                state=indian_states['features'][i]['properties']['ST_NM']
                list_states.append(state)
            st_name=sorted(list_states)
            df['st_name']=st_name
            with right_column:
                st.subheader("{} Users".format(brand))
                st.markdown(df['sum(users_count)'].sum())
                
                st.subheader("Total Registered Users")
                st.markdown(df['sum(users_count)'].sum())
                data =px.choropleth(df,locations='st_name',
                                    title="{}".format(users_data),
                                    geojson=indian_states,
                                    featureidkey='properties.ST_NM',
                                    color='sum(users_count)',
                                    color_continuous_scale='Reds',
                                    scope='asia',
                                    projection="mercator",
                                    hover_data=['year','quarter']
                                   
                 
                                    )
                data.update_geos(fitbounds='locations', visible=False)
                st.plotly_chart(data,use_container_width=True)
        else:
            quarter=st.selectbox(label="**Quarter**",options=("1","2","3","4"))
            year=st.selectbox("**Year**",range(2018,2023))
            query2="select state,year,quarter,sum(registered_users) from {} group by state,year,quarter having year={} and quarter={}".format(users_data,year,quarter)
            df=pd.read_sql_query(query2, connector)
            with urlopen('https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson')as response:
                indian_states=json.load(response)
            list_states=[]
            for i in range(len(indian_states['features'])):
                state=indian_states['features'][i]['properties']['ST_NM']
                list_states.append(state)
            st_name=sorted(list_states)
            df['st_name']=st_name
            with right_column:
                st.subheader("Total Registered Users")
                st.markdown(df['sum(registered_users)'].sum())
                data =px.choropleth(df,locations='st_name',
                                    title="{}".format(users_data),
                                    geojson=indian_states,
                                    featureidkey='properties.ST_NM',
                                    color='sum(registered_users)',
                                    color_continuous_scale='Reds',
                                    scope='asia',
                                    projection="mercator",
                                    hover_data=['year','quarter']
                                   
                 
                                    )
                data.update_geos(fitbounds='locations', visible=False)
                st.plotly_chart(data,use_container_width=True)
                
