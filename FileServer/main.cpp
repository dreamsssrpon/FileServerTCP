//I would like to express my gratitude: prabuddha34 
//I from Russia also i can not speak English very well
//
#include <iostream>
#include "Networker.h"
#include <boost/thread.hpp>
#include <vector>
#include <fstream>
#include <map>
#include <boost/filesystem.hpp>
#include <ifaddrs.h>
#include <boost/process.hpp>
#include <boost/system.hpp>

using namespace std;

constexpr int MAX_PCKG_SIZE = 4096;

int init_epoll(){
    int fd = epoll_create1(0);

    if(fd==-1){
        cout << "Error: epoll not initialized\n";
        while(1){};
    }

    return fd;
}
vector<string> GetIPAdresess(){
    ifaddrs* addr;
    string host_addr = "NULL";

    vector<string> address;

    if(getifaddrs(&addr)!=-1){
        for(auto it = addr;it!=nullptr;it = it->ifa_next){
            if(it->ifa_addr==nullptr||it->ifa_addr->sa_family!=AF_INET){continue;}

            auto addres = (sockaddr_in*)it->ifa_addr;

            string buffer,b;
            buffer+= to_string(it->ifa_name) + ": ";
            b.resize(INET_ADDRSTRLEN);

            inet_ntop(AF_INET,&addres->sin_addr,b.data(),b.size());

            appU32(b,buffer.size());
            address.push_back(buffer+b);
        }
    }
    return address;
}

string GetName(string path){
    string name = "";
    for(int i = path.size()-1;i>=0;--i){
        if(path[i]=='\\'||path[i]=='/'){
            name.append(path.begin()+i+1,path.end());
            return name;
        }
    }
    return path;
}
string DeleteName(string path){
    string b;
    for(uint16_t i = path.size()-1;i>=0;--i){
        if(path[i]=='\\'||path[i]=='/'){
            b.append(path.begin(),path.begin()+i+1);
            return b;
        }
    }   
    return path;
}
string DeleteExt(string path){
    string b;
    for(int i = path.size()-1;i>=0;--i){
        if(path[i]=='.'){
            b.append(path.begin(),path.begin()+i);
            return b;
        }
    }   
    return path;
}

boost::asio::io_context cnt;
void run_cmd(string cmd){
    boost::process::v2::shell sh(cmd);
    boost::process::v2::process pr(cnt,sh.exe(),sh.args());

    pr.wait();
}

void Pack(string src,string dst){
    string cmd = "tar -czf "+dst+".rar"+" -C "+src;
    cout << cmd << endl;
    cout.flush();

    run_cmd(cmd);
}
void Unpack(string src,string dst){
    string cmd = "tar -xzf " + dst+ " -C "+ src;
    cout << cmd << endl;
    cout.flush();

    run_cmd(cmd);
}

string read_file(const char* pth){    
    ifstream f(pth,ios::binary);

    if(!f.is_open()){
        cout << "Can not open file\n";
        return " ";
    }

    f.seekg(0,ios::end);
    int size = (f.tellg());
    f.seekg(0,ios::beg);

    if(size==-1){
        cout << "Error: can not get file size\n";
        return " ";
    }

    string buffer;
    buffer.resize(size);

    f.read(buffer.data(),size);
    f.close();

    return buffer;
}

void WriteFile(string path,string source){
    ofstream f(path,ios::binary);

    if(!f.is_open()){
        cout << "Can not open file\n";
        return;
    }

    f.write(source.data(),source.size());
    f.close();
}
void AddToFile(string path,string source){
    ofstream f(path,ios::binary|ios::app);

    if(!f.is_open()){
        f.open(path,ios::binary);
    }    
    
    if(!f.is_open()){
        cout << "Can not open file path: " << path << endl;
        return;
    }

    f.write(source.data(),source.size());
    f.close();
}

string get_file_section(string path,uint32_t& pos,uint32_t size){
    ifstream f(path,ios::binary|ios::ate);

    if(!f.is_open()){
        return "NULL";
    }

    int f_end = f.tellg();

    if(f_end<=0){
        cout << "Can not get a end pos int file\n";
        return "NULL";
    }

    uint32_t file_end = f_end;

    f.seekg(pos,ios::beg);
    uint32_t file_ = file_end-f.tellg();

    if(size>file_){size = file_;}

    string buffer;
    buffer.resize(size);
    f.read(buffer.data(),size);

    pos+=size;
    return buffer;
}

uint32_t GetSize(const char* pat){
    ifstream fs(pat,ios::binary);

    fs.seekg(0,ios::end);

    if(!fs.is_open()){return 0;}

    uint32_t s = fs.tellg();
    fs.close();
    return s;
}
void RequestAnalaizer(const Request& r){
    cout << "Type " << r.type << " pass " << r.pass << " id " << r.id << endl;
    //cout << "Params " << r.p << endl;
}

#define END_PACKAGE 100
#define MIDDLE_PACKAGE 101
class DataPackage{
public:
    vector<Request> pckg;
    int id;

    uint32_t size1,ind;
    string source,p;

    DataPackage(){}
    DataPackage(string path,int id1){
        //cout << "PATH: " << path << endl;
        p = path;

        id = id1;
        ind = 0;      

        size1 = GetSize(path.data());
    }
    DataPackage(vector<Request> req){
        for(int i = req.size()-1;i>=0;--i){
            source += req[i].p;
        }
        size1 = source.size();
    }
    Request next(){
        string src = get_file_section(p,ind,MAX_PCKG_SIZE);

        Request r(1,1,(src.size()==MAX_PCKG_SIZE?1:0));
        r.p = src;
        if(src == "NULL"){
            Request r(403,403,403);
            r.p = "ERROR: " + p;

            return r;
        }
        return r;
    }
    Request Dnext(){ 
        Request r;

        if(ind>=pckg.size()){
            r = Request(404,404,404);
            r.p = "ERROR\n";
        }else{
            r = pckg[ind++];
        }
        
        return r;
    }
};

/////////////////////////////
void Rsend(int fd,Request r){
    string msg = r.convert();
    Psend(fd,msg);
}
Request Rread(int fd){
    string msg = Pread(fd);
    return Request(msg);
}
constexpr int MAX_CLIENT_SIZE = 30;
constexpr int NOT_FONDED = 404;
constexpr int DOWNOLAD_REQUEST = 0;
constexpr int UPLOAD_REQUEST = 1;

constexpr int REG_DOWNOLOAD_REQ = 1;
constexpr int REG_DOWNOLOADFDIR_REQ = 2;
constexpr int REG_UPLOAD_REQ = 3;

constexpr int GET_NEXT_FILE_FRAME = 4;
constexpr int GET_NEXT_FILE_PATH = 5;
constexpr int DATA_TO_UPLOAD = 6;
constexpr int CRATE_DIRECTORY_REQ = 7;

constexpr int IS_FOLDER = 9;
constexpr int END_FILE = 10;
constexpr int END_FOLDER = 11;
class Cache{
public:

    string source_path;

    map<int,DataPackage> packages;
    int ids;

    map<int,Request> up_req;
    int up_ids;

    Cache(){        
        ids = up_ids = 0;

        packages = map<int,DataPackage>();
        up_req= map<int,Request>();

        source_path = "/home/cpp/FileServer/BASE";
    };
    Request RegDownoloadReq(string download_src){   
        Request response(++ids,0,GET_NEXT_FILE_FRAME);
        
        string src = DeleteName(download_src)+"temp"+to_string(ids);
        string p = DeleteName(download_src)+" "+GetName(download_src);
        Pack(p,src);

        src+=".rar";

        DataPackage pc(src,ids);
        packages[ids] = pc;

        response.pass = GetSize(src.data());
        response.p =" "+ GetName(download_src);
        return response;
    }
    Request RegDownoloadDirReq(string path){
        return Request();
    }
    Request RegUploadReq(string dst){
        Request response(++up_ids,0,DATA_TO_UPLOAD);
        response.p = source_path+"/"+DeleteExt(GetName(dst))+".rar";

        up_req[up_ids] = response;
        return response;
    }
    Request GetNext(int id){
        if(!check(id,0)){
            Request r(0,0,NOT_FONDED);
            r.p =  "PACKAGE NOT FOUNDED\n";
            return r;
        }
        
        Request r = packages[id].next();

        if(r.type!=1){
            deleteReq(id,0);
        }

        return r;
    }
    bool check(int id,int type){
        if(type==0){
            auto it = packages.find(id);
            if(it!=packages.end()){
                return true;
            }
        }
        else{
            auto it = up_req.find(id);
            if(it!=up_req.end()){
                return true;
            }
        }
        return false;
    }
    void deleteReq(int id,int type){
        if(check(id,type)){
            if(type==0){
                run_cmd("rm -r "+packages[id].p);
                packages.erase(packages.find(id));
            }else{
                run_cmd("rm -r "+up_req[id].p);
                up_req.erase(up_req.find(id));
            }
        }
    }
    void reset(){
        packages = map<int,DataPackage>();
        up_req= map<int,Request>();
    }
};

boost::atomic<bool> is_accepted = false;
boost::atomic<int> accepted_fd = -1;

void thread_accepter(int s_fd){
    is_accepted = false;

    accepted_fd = accept(s_fd,NULL,NULL);
    is_accepted = true;
}

boost::thread thr;

class ConnectionHandler{
public:
    uint32_t server_port,max_ev;
    int listen_fd,epoll_fd;
    vector<int> sockets;

    ConnectionHandler(){}
    ConnectionHandler(uint32_t p,uint32_t max_clients,uint32_t max_events){
        server_port = p;
        max_ev = max_events;

        listen_fd = socket(AF_INET,SOCK_STREAM,0);
        epoll_fd = init_epoll();

        sockaddr_in addr;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(p);

        if(::bind(listen_fd,(sockaddr*)&addr,sizeof(addr))){
            cout << "Can not bind addres struct for socket\n";
            exit(-1);
        }
        if(listen(listen_fd,max_clients)){
            cout << "Can not change socket mode\n";
            exit(-1);
        }

        thr = boost::thread(thread_accepter,listen_fd);
    }
    void AddSocket(int fd) {
        epoll_event ev;
        ev.events = EPOLLIN|EPOLLHUP|EPOLLRDHUP|EPOLLERR;
        ev.data.fd = fd;

        if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,fd,&ev)==-1){
            cout << "Can not add socket to epoll\n";
            cout.flush();
            return;
        }
        cout << "Socket added \n";
        //sockets.push_back(fd);
    }   
    void delete_socket(int fd){
        if(epoll_ctl(epoll_fd,EPOLL_CTL_DEL,fd,NULL)){
            cout << "Can not delete a socket\n";
            cout.flush();
            return;
        }
        cout << "Socket deleted\n";
        cout.flush();
    }
    vector<epoll_event> GetEvents(){
        vector<epoll_event> ev;
        ev.resize(max_ev);

        int evs = epoll_wait(epoll_fd,ev.data(),max_ev,0);

        if(evs==-1){
            return vector<epoll_event>();
        }

        ev.resize(evs);
        return ev;
    }
    int acceptd(){
        if(is_accepted){
            int fd = accepted_fd;
            thr = boost::thread(thread_accepter,listen_fd);

            sockets.push_back(fd);
            AddSocket(fd);

            return fd;
        }
        return -1;
    }
    void free(){
        for(auto it : sockets){
            close(it);
        }
        close(epoll_fd);
        close(listen_fd);
    }
};  
string dirs(string path){
    boost::filesystem::recursive_directory_iterator iter(path);

    string paths;
    for(auto it:iter){
        paths+= it.path().string() + "\n";
    }   
    return paths;
}

class FileServerTCP{
public:
    ConnectionHandler handl;
    Cache cache;

    FileServerTCP(){}
    FileServerTCP(uint32_t p){  
        handl = ConnectionHandler(p,MAX_CLIENT_SIZE,20);
        cache = Cache();
        printServerInfo();
    }
    void handle(){
        handl.acceptd();

        vector<epoll_event> evs = handl.GetEvents();
        for(uint8_t i =0 ; i < evs.size();++i){
            if(evs[i].events&EPOLLHUP||evs[i].events&EPOLLRDHUP||evs[i].events&EPOLLERR){
                cout << "Delete connection " << evs[i].data.fd << endl;
                handl.delete_socket(evs[i].data.fd);
                continue;
            }
            Request r = Rread(evs[i].data.fd);
            RequestHandler(evs[i].data.fd,r);
        }
    }
    void printServerInfo(){
        cout << "--------------SERVER INFO--------------" << endl;
        vector<string> adrss = GetIPAdresess();
        for(auto it:adrss ){
            cout << it << endl;
        }

        cout << "Port: " << handl.server_port << endl;
        cout << "Max package size: " << MAX_PCKG_SIZE << endl;
        //cout << "Source dir: " << src_dir << endl;
    }
    void RequestHandler(int fd,Request req){
        Request response;

        if(req.type == REG_DOWNOLOAD_REQ){
            response = cache.RegDownoloadReq(req.p);
            cout << "----------------Dowonoload request registrated----------------\n";
            cout <<"Downoload req info: id: " << response.id << " path: " << req.p << endl;
        }
        else if(req.type == GET_NEXT_FILE_FRAME){
            response = cache.GetNext(req.id);
        }
        else if(req.type == REG_UPLOAD_REQ){
            response = cache.RegUploadReq(req.p);
            cout << "----------------Upload request registrated----------------\n";
            cout <<"Upload req info: id: " << response.id << " path: " << req.p << endl;
        }else if(req.type == END_FILE){
            if(!cache.check(req.id,1)){
                response = Request(404,404,404);
                cout << "Upload not succesfuly\n";
                cout.flush();
            }
            
            string path = cache.up_req[req.id].p;

            cout << "Upload stop\n";
            cout.flush();

            cout << "Unpacking archive...";
            Unpack(DeleteName(path)+" "+ req.p,path);
            cout << "Done" << endl;

            cache.deleteReq(req.id,1);
        }
        else if(req.type == DATA_TO_UPLOAD){
            response = Request(1,1,1);
            if(!cache.check(req.id,1)){
                response = Request(404,404,404);
                req.p = "";
            }  
                
            AddToFile(cache.up_req[req.id].p,req.p);            
        }else{
            response = Request(0,0,NOT_FONDED);
        }
        string resp = response.convert(); 
        response = Request(resp);       
        Psend(fd,resp);
    }
    void free(){
        handl.free();
    }
};

FileServerTCP serv(8080);
int main(){
    //DataPackage pckg("/home/cpp/FileServer/data/hell.txt",2);

    while(1){
        serv.handle();
    }    
    return 0;
}