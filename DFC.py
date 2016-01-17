import os
import hashlib
import socket
import collections
from ConfigParser import ConfigParser
import time
import sys


config_file=sys.argv[1]

config = ConfigParser()
config.read(config_file)
config_dictionary = {}
for section in config.sections():
    config_dictionary[section] = {}
    for option in config.options(section):
        config_dictionary[section][option] = config.get(section, option)

ip1,port1=(config_dictionary["ports"]["df1"]).split(":")
ip2,port2=(config_dictionary["ports"]["df2"]).split(":")
ip3,port3=(config_dictionary["ports"]["df3"]).split(":")
ip4,port4=(config_dictionary["ports"]["df4"]).split(":")

get_dict={}
chunk_dict={}
port_1=int(port1)
port_2=int(port2)
port_3=int(port3)
port_4=int(port4)
port_names=[port_1,port_2,port_3,port_4]
server_IP=str(ip1)

def func_list(list_data):
    list_dict={}
    j=500

    for value in port_names:   
        data_list=[]

        print("Listing data from server"+str(value))
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("testing....1")
        try:
            sock.connect((server_IP,value))
        except:
            continue
        print("testing....2")

        sock.sendall(list_data)
        
        i=0
        while True:
            sock.settimeout(1.0)
            try:  
                i=i+1
                print("receiving from server")
                received_list_data=sock.recv(4096000)
                data_list=received_list_data.split("|||")
                print(data_list)

                
                for element in data_list:
                    key_temp=str(j)+"__"+element[1:-2]
                    list_dict[key_temp]=element[-1]
                    j=j+1
                            
                            
                
            except:
                print("Entered except statement")
                print(i)
                if i>3:
                    break
                continue
    
    temporary_list=[]
    print(list_dict)
    print(len(list_dict))
    for key in list_dict.keys():
        temporary_list.append(key[5:])
    print(temporary_list)
    set_object=set(temporary_list)
    #print(set_object)
    
    
    print("\n\n*****************LIST OF FILES**********************")
    for i in set_object:
        list_a=[]
        for key in list_dict.keys():
            if i==key[5:]:
                list_a.append(list_dict[key])
                
                
        
        if len(set(list_a))==4:
            print(i+" --------- "+"COMPLETE")
        else:
            print(i+" --------- "+"INCOMPLETE")
            
        
                        

def func_get(get_data,client_file_name):
    print("Entered GET Function")
    for value in port_names:   
        if len(get_dict)==4:
            print("OPTIMIZING NETWORK LOAD.... ONE CYCLE SAVED.....")
            break
        print("getting data from server..."+str(value))
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("testing....1")
        try:
            sock.connect((server_IP,value))
        except:
            continue
            
        print("testing....2")
        
        sock.sendall(get_data)
        
        
        i=0
        while True:
            
            sock.settimeout(1.0)
            try:  
                i=i+1
                print("receiving from server")
                received_server_data=sock.recv(4096000)
                a,b=received_server_data.split("|||",1)
                get_dict[a]=b
                print(get_dict.keys())

                
            except:
                print("Entered except statement")
                print(i)
                if i>3:
                    break
                continue
            
        sock.close()
        print("socket closed\n")
        print("Initiating process to write the file")
        
        
    file_name="received_file_"+client_file_name
    fh=open(file_name,"wb")
    
    for keys in sorted(get_dict.keys()):
        #print(get_dict[keys])
        fh.write(get_dict[keys])
    fh.close()
    print("Received file successfully written")
            

def func_put(chunk_dict,hash_mod,deq):
    print("Entered sending function (PUT)")
    
    if hash_mod==0:
        deq.rotate(0)
    elif hash_mod==1:
        deq.rotate(1)
    elif hash_mod==2:
        deq.rotate(2)
    elif hash_mod==3:
        deq.rotate(3)
    print("rotation of deque done\n")
    #print(deq)
    t=0   
    for value in port_names:   
        print("sending data to server..."+str(value))
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("testing....1")
        sock.connect((server_IP,value))
        print("testing....2")
        sock.sendall(deq[t][0])
        time.sleep(0.3)
        sock.sendall(deq[t][1])
        t=t+1
        
        while True:
            sock.settimeout(1.0)
            try:  
                print("receiving from server")
                received_server_data=sock.recv(4096000)
                print(received_server_data)
                           
            except:
                print("Entered except statement")
                break
            
        sock.close() 
        print("socket closed\n")
        
    
    
def main(): 
    
    config_file=sys.argv[1]
    print("ENTERED MAIN")
    config = ConfigParser()
    config.read(config_file)
    config_dictionary = {}
    for section in config.sections():
        config_dictionary[section] = {}
        for option in config.options(section):
            config_dictionary[section][option] = config.get(section, option)
            
    username=config_dictionary["credentials"]["username"]
    password=config_dictionary["credentials"]["password"]
    
    

    method=raw_input("Please enter the selected method: PUT, GET, LIST\n")
    
    if method=="put":
        
        client_file_name=raw_input("Please enter the file name\n")
        
        chunk_size=((int(os.path.getsize(client_file_name)))/4)+1
        print("chunk size is {}".format(chunk_size))
        
        file_hash=hashlib.md5(open(client_file_name, 'rb').read()).hexdigest()
        hash_decimal=int(file_hash,16)
        hash_mod=hash_decimal%4
        
        print("The modulus of hash value is {}".format(hash_mod))
        
        fh=open(client_file_name,'rb')
        
        piece_num=1
        for filepiece in read_in_parts(chunk_size,fh):
            chunk_dict[piece_num]=username+"|||"+password+"|||"+method+"|||"+client_file_name+"|||"+str(chunk_size)+"|||"+str(hash_mod)+"|||"+str(piece_num)+"|||"+filepiece
            piece_num=piece_num+1
            if piece_num==5:
                break 
        print("Chunks read into list in four parts")
        
        deq=collections.deque()
        deq.append([chunk_dict[1],chunk_dict[2]])
        deq.append([chunk_dict[2],chunk_dict[3]])
        deq.append([chunk_dict[3],chunk_dict[4]])
        deq.append([chunk_dict[4],chunk_dict[1]])
        
        print("deque created")
        
        func_put(chunk_dict,hash_mod,deq)
    
    elif method=="get":
        print("get method code")
        client_file_name=raw_input("Please enter the file name\n")
        get_data=username+"|||"+password+"|||"+method+"|||"+client_file_name
        
        func_get(get_data,client_file_name)
        
        
        
        
        
    elif method=="list":
        print("list method code")
        list_data=username+"|||"+password+"|||"+method
        func_list(list_data)
        
        
    else:
        print("Error! No such option exists.") 
        
def read_in_parts(chunk_size,fh):
        while True:
            data = fh.read(chunk_size)
            #print(data)
            yield data

if __name__=="__main__":
    main()
    