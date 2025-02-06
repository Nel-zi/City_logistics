#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import necessary dependencies
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


# In[2]:


# Extraction layer
City_logistics_df = pd.read_csv(r'Raw_dataset/City_logistics_data.csv')


# In[3]:


City_logistics_df.head()


# In[4]:


City_logistics_df.info()


# In[5]:


City_logistics_df.columns


# In[ ]:


# Data cleaning and transformation layer
City_logistics_df.fillna({
    'Unit_Price' : City_logistics_df['Unit_Price'].mean(),
    'Total_Cost' : City_logistics_df['Total_Cost'].mean(),
    'Discount_Rate' : 0.0,
    'Return_Reason' : 'Unknown'
}, inplace=True)


# In[9]:


City_logistics_df.info()


# In[10]:


# Converting the data column from string object to dateTime
City_logistics_df['Date'] = pd.to_datetime(City_logistics_df['Date'])


# In[14]:


# Now since the cleaning is done, the individual tables should be created for the transformation
# Creating the customer table
customer = City_logistics_df[['Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address']].copy().drop_duplicates().reset_index(drop=True)

customer.head()


# In[13]:


City_logistics_df.info()


# In[17]:


# Creating the product table
product = City_logistics_df[['Product_ID', 'Product_List_Title', 'Quantity','Unit_Price', 'Discount_Rate']].copy().drop_duplicates().reset_index(drop=True)

product


# In[20]:


City_logistics_df.columns


# In[21]:


# Creating the Transaction_fact_table
transaction_fact = City_logistics_df.merge(customer, on=['Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address'], how='left') \
                                  .merge(product, on=['Product_ID', 'Product_List_Title', 'Quantity','Unit_Price', 'Discount_Rate'], how='left') \
                                  [['Transaction_ID', 'Date', 'Customer_ID', 'Product_ID', 'Total_Cost', 'Sales_Channel', 'Order_Priority', \
                                    'Warehouse_Code', 'Ship_Mode', 'Delivery_Status', 'Customer_Satisfaction', 'Item_Returned', 'Return_Reason', \
                                    'Payment_Type', 'Taxable', 'Region', 'Country']]


# In[31]:


transaction_fact.info()


# In[32]:


# Down the line, i found that the datetime conversion we did earlier has a little format issue with parquet...
# ...so we should Converting it to a different format
transaction_fact['Date'] = transaction_fact['Date'].astype('datetime64[us]')


# #### Data Loading layer Azure

# In[33]:


# Saving the tables as CSV files
customer.to_csv(r'Clean_dataset/customer.csv', index=False)
product.to_csv(r'Clean_dataset/product.csv', index=False)
transaction_fact.to_csv(r'Clean_dataset/transaction_fact.csv', index=False)


# In[38]:


import io
from azure.storage.blob import BlobServiceClient, BlobClient 


# In[45]:


# Setting up Azure connection
load_dotenv()
connect_str = os.getenv('CONNECT_STR')
container_name = os.getenv('CONTAINER_NAME')

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)


# In[43]:


# Next is to create a function that loads the data into Azure blob storage as a parquet file
def upload_df_to_blob_as_parquet(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client =  container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print(f'{blob_name} uploaded to Blob storage successfuly')


# In[46]:


upload_df_to_blob_as_parquet(customer, container_client, 'Clean_dataset/customer.parquet')
upload_df_to_blob_as_parquet(product, container_client, 'Clean_dataset/product.parquet')
upload_df_to_blob_as_parquet(transaction_fact, container_client, 'Clean_dataset/transaction_fact.parquet')

