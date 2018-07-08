
# coding: utf-8

# In[3]:


sc


# In[5]:


filepath = "D:\Study\Hadoop\AAPL.csv"
#we are rading file from windows
#Reading file from local machine of Windows


# In[7]:


apple = sc.textFile(filepath)


# In[8]:


apple


# In[9]:


type(apple)


# In[10]:


apple.take(5)


# In[11]:


apple.count()


# In[12]:


apple.first()


# In[13]:


apple.collect()

