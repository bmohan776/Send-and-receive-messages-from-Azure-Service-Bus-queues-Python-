import logging
import json
import time 
import configparser
import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import azure.functions as func

from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
import cx_Oracle


def get_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    CONNECTION_STR  = config['DEFAULT']['CONNECTION_STR'].strip('"').strip("'")
    QUEUE_NAME = config['DEFAULT']['QUEUE_NAME'].strip('"').strip("'")
    return CONNECTION_STR, QUEUE_NAME


## Key Valut data
def get_from_keyvault(secret_name):
    """
    CLIENT_ID = config('CLIENT_ID')
    TENANT_ID = config('TENANT_ID')
    CLIENT_SECRET = config('CLIENT_SECRET')
    KEYVAULT_NAME = config('KEYVAULT_NAME') """

    
    CLIENT_ID = os.getenv('CLIENT_ID')
    TENANT_ID = os.getenv('TENANT_ID')
    CLIENT_SECRET = os.getenv('CLIENT_SECRET')
    KEYVAULT_NAME = os.getenv('KEYVAULT_NAME')
    
    
    keyvault_uri = f"https://{KEYVAULT_NAME}.vault.azure.net"

    _client_credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    _secret_client = SecretClient(
        vault_url=keyvault_uri, credential=_client_credential)

    return _secret_client.get_secret(secret_name).value


# init credentials
DB_USERNAME = get_from_keyvault(secret_name='func-db-username')
DB_PASSWORD = get_from_keyvault(secret_name='func-db-password')
DB_HOST = get_from_keyvault(secret_name='func-db-host')
DB_PORT = get_from_keyvault(secret_name='func-db-port')
DB_SID = get_from_keyvault(secret_name='func-db-sid')

db_connection_string = f"{DB_HOST}:{DB_PORT}/{DB_SID}"


try:
#create a connection
    conn = cx_Oracle.connect(
    DB_USERNAME,
    DB_PASSWORD,
    db_connection_string)

except Exception as err:
    print('exception occured while creating a connection', err)
else:
    def data_load_prc(sender):    
        try:
            cur=conn.cursor()
            data = ['input value1',' input value2']
            cur.callproc('Proc Name', data)

        except Exception as err :
            print('exception raised while execting the proc call', err)
        else:
            print('Procedure executed.')

        finally:
            cur.close()    
finally:        
        conn.close()
        

def send_single_message(sender):
    message = ServiceBusMessage("Single Message")
    sender.send_messages(message)
    print("Sent a single message")

def send_a_list_of_messages(sender):
    messages = [ServiceBusMessage("Message in list") for _ in range(10)]
    sender.send_messages(messages)
    print("Sent a list of 5 messages")

def send_batch_message(sender):
    batch_message = sender.create_message_batch()
    for _ in range(10):
        try:
            batch_message.add_message(ServiceBusMessage("Message inside a ServiceBusMessageBatch"))
        except ValueError:
            # ServiceBusMessageBatch object reaches max_size.
            # New ServiceBusMessageBatch object can be created here to send more data.
            break
    sender.send_messages(batch_message)
    print("Sent a batch of 10 messages")
    
CONNECTION_STR, QUEUE_NAME = get_config()    

servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)

# procedure messages to service bus queue
with servicebus_client:
    sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
    with sender:
        message = send_single_message(sender)
        send_a_list_of_messages(sender)
        send_batch_message(sender)

print("Done sending messages")
print("-----------------------")


            
#Consuming the message from Service Bus 
def main(msg: func.ServiceBusMessage):
    with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=5)
        with receiver:
            for msg in receiver:
                print("Received: " + str(msg))
                receiver.complete_message(msg)
                #Pass the queue message to database table
                data_load_prc(msg)
                logging.info('Python ServiceBus queue trigger processed message: %s',
                     msg.get_body().decode('utf-8'))
            
    
