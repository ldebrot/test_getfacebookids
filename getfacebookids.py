#!/usr/bin/env python
# coding: utf-8

# In[170]:


#DOCUMENTATION
#https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html

#input : facebook unique handles
#output : dataframe as JSON: handle, id

#First inserts input handles into phantombusterbuffer Google Spreadsheet
#Then sends request to Phantombuster agent, which goes through Google Spreadsheet
#Phantombuster responds with json data : url,id,originalUrl
#Module responds with False if unsucessful, and with a pd Dataframe(cols=["handle", "id"]) if successful


# In[164]:


#import stuff
import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials
import json
import pandas as pd


# In[165]:


#fills in input in Google Spreadsheet
def preparegspread(input_array, input_credentialsjson, input_gspreadbuffersheet):
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        input_credentialsjson = {
            "type": "service_account",
            "project_id": "meta-gateway-245215",
            "private_key_id": "727bdb95b22d85fee110b192caa46fab7f710e8f",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC2b9nMSgbZK3CH\nMLK8LLqdDKWSeSbciG9W84A9ZwqWiuaUD2WMBgyn94sx6kTxj2ov87ocLUPwZdtE\nKlZ4aOr5bPsOtSeMPNWBTf41IkIaAgBsBXurJKKekdO13wgnicKQjbL5HmiTlbIe\nq1CUeprJovL0J+n5rkUhRf7rRVTA5P/LqDvdlDd8aM8EvZzkJ3pea3oClVVrTcCP\nxWwUjxMX9mDHUl4snu+Lucw4n7bRn2P3EEkjj9nizPop9xtPyJIF6ZkhNO34cm6E\naA/PRtQNE4/5OULgX7FUvmUJk9muAYQJytgJhDaYrF6PjNDWkeKn5Pp9MMr2Ovwd\n8E4VFWOtAgMBAAECggEAOx7oH9Hlgj8Xilf3tA9ysi5FAcCNHW6BVlY2uq9HuR1u\nN5D9wXC8bTpFO3nFudpV+RAswbDRkXgQpHhr6T1m2Em+2jdOh523Ot4BgUqGzVLU\nvCzStNV8dZKgXxHV3KpCCQJCe4SlMx6RyGqQBG3u92T9Aq5knmshL/U09R1Up+WB\nxuqbjqSgVRMOggKMI9xELZCaEEFGyIwV3NF8fdDKNX84MbXoIaC2sUsMrZlcUF4k\nZ9wU9zgqss781n6YEpFT5LfznLNvZWRY1PeuXeBXOsrDY0DB9i+H9J19ijmboCJF\n6wxCzz05ZVQbi2eM+Sflzv6+IOBL8Y6jooBrI2ONaQKBgQDlUbQq2x/tH3991a9A\n9vKCYdlLNopGBig1zyUjohqBxXkA08znPwixazVKs4UsRKwVyB9Opx/pwOFUZnEg\nV5nqjdy8J+yax2i6rkfsNeg4+Cyt7WtmaayFwo5m/VLg10l4JtFZ0F56RfcE4WSu\nmVzoWDClyurthNqHdw76xJWSXwKBgQDLqcEfAEhIOJB2CBwAk9WrnYdpP3qeQngh\nUYV7x+bctCIIymRwWi6ZhPQZ2U+/fpOi+GEwo/XVVklJRMWR2IMMvrY9eBnFkqEM\nUe/JdzNLgzHhRkN+BbaLKAw9Hfu2WkzWtpDkLkxrv5W8C7SnNXU2mBG17W2b2PXu\n78gERHw9cwKBgB8kTVdUkq1dDa9FCZvE7YIPT7cLHRQNWgCUtPJ13SuqaZhXKwhs\nYv3VFVOjtX4hhU0f9EB2KbtQ2kjf8JT30Ist4MAcm+EZG3velx3Y5ER4T3j/OQqb\nZzZzCWurE0o9cljCzMNprKrmPXmIMmgTo65Z66FYQTcoR5UF8h7T+4v5AoGBAJi3\njyfm90yolbLcO9s2/9czRdHnFtRLdXdOgPCjonrG3GqJEa2qAh1+M+iaKpGmoaBn\n7cEigXnavROi7R+DIinRbChFwdWEMR1i55LDvoQWObX6ESwTrqFEg4Dk1dYUel/b\nAGUKTqXRQL+Ea4+ip2UrvMQV6MYfd7MWvH5uuFb/AoGASIHybGkwB6g1nOuDctXL\n0EkmqHRkFgX/ieDBC8g/GdfawL1dRJXek5+VG07T/XeDV1iWSI1qKAelubVjXcng\nCnzqHkmH62yytJkfSOdG1Hs7Ry1UdlN7DwbiMkwg+TiuK2SDp1AR+9uEDHDih2Q9\ng4m12PE5qSvpy0n0kTO5SBs=\n-----END PRIVATE KEY-----\n",
            "client_email": "angiegoogle@meta-gateway-245215.iam.gserviceaccount.com",
            "client_id": "111191572995816261130",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/angiegoogle%40meta-gateway-245215.iam.gserviceaccount.com"
        }
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(input_credentialsjson, scope)
        gc = gspread.authorize(credentials)
        gspreadsheet = gc.open(input_gspreadbuffersheet)

        #Clear, then fill in all cells
        gspreadsheet.sheet1.clear()
        temp_range = "".join(['Sheet1!A1:A',str(len(input_array))])
        gspreadsheet.values_update(
            temp_range,
            params={
                'valueInputOption': 'USER_ENTERED'
            },
            body={
                'values': [[x] for x in input_array]
            }
        )
    except:
        print("error with Google")
    return True


# In[166]:


#Launches Phantombuster Agent on newly modified Google Spreadsheet (which is specified on the Phantombuster platform)
def launchphantombusteragent(input_phantombusteragenturl, input_phantombusterkey):
    querystring = {
        "output":"result-object-with-output",
        "saveArgument":"false"
    }
    #must choose result-object-with-output to get output from API
    #will not save arguments sent to Phantombuster
    temp_headers = {'x-phantombuster-key-1': input_phantombusterkey,
                    'accept': "application/json",
                   }
    response = requests.request("POST",
                                url=input_phantombusteragenturl,
                                headers=temp_headers,
                                params=querystring)
    return response.text


# In[167]:


#Checks if input is a valid list with at least one item
def isinputvalid(input):
    temp_response = False
    if isinstance(input, list) == True:
        if len(input) > 0:
            temp_response = True
    return temp_response


# In[168]:


#Checks Phantombuster response and, if successful, extracts data
def getresponsearray(input_response):
    try:
        temp_json = json.loads(input_response)
        if temp_json["status"] == "success":
            print("Phantombuster response is successful")
            input_response_json = json.loads(input_response)
            print("input_response_json", input_response_json)
            temp_df = pd.DataFrame(input_response_json["data"]["resultObject"])
            print("temp_df", temp_df)
            response_df = pd.DataFrame({"handle": temp_df["originalUrl"].tolist(), "id": temp_df["id"].tolist()})
            return response_df.to_json()
    except:
        return False


# In[169]:


#Runs the analysis
def runanalysis(input_array):
    if isinputvalid(input=input_array) == True:
        temp_credentialsjson = 'angiegoogle-727bdb95b22d.json'
        temp_gspreadbuffersheet = "phantombusterbuffer"
        temp_phantombusteragenturl = "https://phantombuster.com/api/v1/agent/133142/launch"
        temp_phantombusterkey = "sfA9U6PKyp5LRlKUUOITa4kPsz0jbLVR"

        preparegspread(input_array = input_array,
                          input_credentialsjson = temp_credentialsjson,
                          input_gspreadbuffersheet = temp_gspreadbuffersheet)
        
        temp_response = launchphantombusteragent(input_phantombusteragenturl = temp_phantombusteragenturl,
                                                 input_phantombusterkey = temp_phantombusterkey)
        
        temp_response = getresponsearray(input_response = temp_response)
    else:
        return False
    return temp_response

