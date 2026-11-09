#pragma once

#include <iostream>
#include <boost/asio.hpp>
#include <map>
#include <string.h>
#include <sqlite3.h>

//#define GNU_SOURCE
#include <sys/socket.h>
#include <sys/unistd.h>
#include <sys/epoll.h>
#include <netinet/in.h>

using namespace std;
using namespace boost;
using namespace asio;

#define NON_BLOCK 1

void send_all(int fd,char* buffer,uint32_t size,int flag){
    int sended = 0;
    for(uint32_t sent = 0;sent<size;){
        sended=send(fd,buffer+sent,size-sent,MSG_NOSIGNAL);//sended size
        if(flag==NON_BLOCK&&sended<=0){//NON_BLOCK mode
            return;
        }
        sent+=sended;//add to global_sended bytes
   }
}
string read_all(int fd,uint32_t size,int flag){
    string str;//resrved bytes

    str.resize(size);

    for(ssize_t i = 0,R; i < size;){
        R=recv(fd,str.data()+i,size-i,MSG_NOSIGNAL);//update recvieted bytes
        if(R<=0&&flag==NON_BLOCK){//NON_BLOCK mode
            return str.data();
        }
        i+=R;//update global recevieted bytes
    }
    return str;//return a ptr because we need read bytes 
}

void Psend(int fd,string msg){//POSIX send 
    uint32_t size = (msg.size());//size in BE

    send_all(fd,(char*)&size,sizeof(uint32_t),0);//send size
    send_all(fd,&msg[0],msg.size(),0);//send body
}
string Pread(int fd){
    string d = read_all(fd,sizeof(uint32_t),NON_BLOCK);
    uint32_t si = (*((uint32_t*)&d[0]));//read size in BE

    if(si>10000){
        return " ";
    }

    d = read_all(fd,si,0);//read bytes

    return d;
}

void B_send(ip::tcp::socket& s,string msg){//Boost::asio send call a Posix send with native_handle()
    Psend(s.native_handle(),msg);
}
string B_read(ip::tcp::socket& s){//Boost::asio recv call a Posix read with native_handle()
    return Pread(s.native_handle());
}

////////////
void appU32(string& data,uint32_t i){//append and reserved space for U32 in binary mode
    string tmp;//Create substring 
    tmp.resize(sizeof(uint32_t));//reserv bytes for U32

    memcpy(&tmp[0],&i,sizeof(uint32_t));//Copy U32 in substr
    data+=tmp;//add substr in main str
}

uint32_t readU32(uint32_t& read_pos,string read_data){//Read U32 for string in binary mode
    uint32_t buffer = 0;//reserved bytes for buffer

    if(read_pos>=read_data.size()){//check size
        return 1;
    }

    memcpy(&buffer,read_data.data()+read_pos,sizeof(uint32_t));//copy bytes in buffer

    read_pos += sizeof(uint32_t);//add 4 bytes 
    return (buffer);//returned buffer
}
//status defined

#define S_ADMINISTRATOR 9
#define S_BLOCKING 1
#define S_MAX 8//My status 
#define S_MUTE 2
#define S_DELETEDACC 3

class User{
public:
    string name;
    uint32_t id,password,status;

    User(){id=password=status=S_DELETEDACC;};
    User(string n,uint32_t id1,uint32_t pass){
        name  = n;
        id = id1;
        password = pass;
    }

};
//Request types
#define SEND 1
#define AUTORIZ 2
#define REGISTRATION 3
#define EXIT 4
#define GUA 5
#define GETCACHE 6
#define GMC 7

class Request{
public:
    uint32_t id,pass,type;
    string p,src;

    Request(){}
    Request(uint32_t i,uint32_t password,uint32_t ty){
        id = i;
        pass = password;
        type = ty;  
    }
    Request(string data){        
        uint32_t pos = 0;

        id = readU32(pos,data);//Read data fixed header size = 16 bytes
        pass = readU32(pos,data);
        type = readU32(pos,data);        
        uint32_t size = readU32(pos,data);//params size

        //cout << "size " << size << endl;
        p.append(&data[pos],size);//set params
    }

    string convert(){
        string buffer;

        appU32(buffer,id);//append bytes for reading
        appU32(buffer,pass);
        appU32(buffer,type);
        appU32(buffer,p.size());

        buffer += p;
        return buffer;
    }

};

class MSG{
public:
    uint32_t from,id;
    string msg;
    MSG(){
        from = -1;
    }
    MSG(uint32_t i1,uint32_t i2,string m){
        from = i1;
        id = i2;
        msg = m;
    }

    string convert(){
        string buffer;
        uint32_t s = msg.size();

        appU32(buffer,from);
        appU32(buffer,s);

        buffer+=msg;
        return buffer;
    }

    void print(){
        cout << from << ">: " << msg << endl;
    }
    string con_print(){
        return string(to_string(from) + " >: " + msg + '\n');
    }
};

MSG readMSG(uint32_t& pos,string d){
    uint32_t from,size;

    from = readU32(pos,d);//read sender
    size = readU32(pos,d);//read msg size

    MSG msg;
    msg.from = from;

    msg.msg.resize(size);//reserv bytes
    memcpy(msg.msg.data(),d.data()+pos,size);//copy data 

    pos+=size;

    return msg;
}
//map<uint32_t,MSG> load_cache(string file){}