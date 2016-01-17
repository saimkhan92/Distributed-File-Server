import socket
import thread
temp_dict={}
from ConfigParser import ConfigParser
import os
import sys

config_dictionary1={}
argument1=sys.argv[1]          #"\DFS3"

def main():
    
    while True:
        server_port=int(sys.argv[2])     #1233
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("127.0.0.1", server_port))
        except:
            print("PORT ALREADY IN USE. (also check that you entered a valid IP if you get multiple errors)")
            raise SystemExit
        sock.listen(10)
        print("socket is now listening\n")
        clientSocket,clientAddress= sock.accept()
        thread.start_new_thread(server_func,(clientSocket,clientAddress))
        print('Creating new thread ')
    sock.close
    print("main socket closed")
    
def server_func(clientSocket,clientAddress):  
    
    config1 = ConfigParser()                                # read password into a dictionary
    config1.read("serverside.ini")
    config_dictionary1 = {}
    for section in config1.sections():
        config_dictionary1[section] = {} 
        for option in config1.options(section):
            config_dictionary1[section][option] = config1.get(section, option)
    
    print(config_dictionary1)
    
    print("Connection from {}".format(clientAddress))
    
    received_dict={}
    i=1
    while True:                                              # Receive data
        clientSocket.settimeout(1.0)
        try:
            i=i+1
            print("receiving data at server 1")
            received_data= clientSocket.recv(409600)
            if len(received_data)==0:
                break
            received_data_list=received_data.split("|||",7)
            print(received_data_list)
            received_dict[i]=received_data_list
            i=i+1
            
        except:
            print("entered except")
            print(i)
            if i>3:
                break
            continue
        
        print("still inside while loop")
        
    print("Entering password authentication phase")
    password_flag="not_matched"                             # Password Authentication
    for received_list in received_dict.values():
        for uname in config_dictionary1["credentials"]:
            #print(received_list)
            if uname==received_list[0]:
                if config_dictionary1["credentials"][uname]==received_list[1]:
                    print("PASSWORD MATCHED")
                    #clientSocket.send("PASSWORD AUTHENTICATION SUCCESSFUL!")
                    password_flag="matched"
                    
    if password_flag=="matched":
        for received_list in received_dict.values():            #for both received chunks
            
            if received_list[2]=="put":                         #put operation
                print("operation for put")
                save_name=argument1[1:len(argument1)]+"\\"+received_list[0]+"\\"+"."+received_list[3]+"."+received_list[6]
                current_path=os.getcwd()
                final_path=os.path.join(current_path,save_name)
                if not os.path.exists(os.path.dirname(final_path)):
                    os.makedirs(os.path.dirname(final_path))
                fh=open(final_path,"wb")
                fh.write(received_list[7])
                
                                
            elif received_list[2]=="list":
                print("operation for list")
                print("operation for list")
                print(received_list)
                directory_path=argument1[1:len(argument1)]+"\\"+received_list[0]
                print(directory_path)
                file_names=os.listdir(directory_path)
                
                send_list=""
                for element in file_names:
                    #new_element=element[1:-2]
                    print(element)
                    send_list=send_list+"|||"+element
                
                send_list=send_list[3:]
                clientSocket.send(send_list)
                    
                
                
                
                
                
            elif received_list[2]=="get":
                print("operation for get")
                print(received_list)
                
                directory_path=argument1[1:len(argument1)]+"\\"+received_list[0]
                print(directory_path)
                file_names=os.listdir(directory_path)
                
                for element in file_names:
                    new_element=element[1:-2]
                    print(new_element)
                    if received_list[3]==new_element:
                        file_path=directory_path+"\\"+element
                        fh=open(file_path,"rb")
                        data=fh.read()
                        MESSAGE=element[-1:]+"|||"+data
                        clientSocket.send(MESSAGE)
                        print("File piece {} sent".format(element[-1:]))
                
                
                
                
                
                
                
                
                
                
                
    else:
        password_flag="not_matched"
        clientSocket.send("ERROR! Password Incorrect")
        print("ERROR AT SERVER'S SIDE PASSWORD NOT MATCHED")
            
            
            
            
            
            
                
        
    
if __name__=="__main__":    
    main()
    
    
    
    
    
    
    