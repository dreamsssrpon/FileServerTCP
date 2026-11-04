#include <iostream>
#include "Networker.h"
#include <boost/thread.hpp>
#include <vector>
#include <fstream>
#include <map>
#include <boost/filesystem.hpp>

using namespace std;

#define MAX_SIZE 50

#define DOWNOLOAD_DATA 1
#define UPLOAD_DATA 2
#define DELETE_SESSIA 3
#define GET_FILE_LIST 4
#define NEXT 5
#define CLOSE_CONNECTION 6
#define NEXT_FILE 7
#define DIR 8
#define END_DIR 9
#define DOWNOLOAD_DIR 10
#define REGISTRATE_UPLOAD_REQ 11

int socket_fd,ep_fd;

vector<int> fds;

int init_epoll(){
    int fd = epoll_create1(0);

    if(!fd){
        cout << "Error: epoll not initialized\n";
        while(1){};
    }

    return fd;
}

void add_socket(int fd,int e_fd){
    if(fds.size()>=MAX_SIZE){
        cout << "Socket not added\n";
        return;
    }

    epoll_event ev;
    ev.data.fd = fd;
    ev.events = EPOLLIN|EPOLLHUP;

    if(epoll_ctl(e_fd,EPOLL_CTL_ADD,fd,&ev)){
        cout << "Socket not added\n";
        return;
    }

    cout << "Socket added\n";
    fds.push_back(fd);
}

void delete_socket(int fd,int e_fd){
    if(fds.size()==0){
        cout << "Error: can not delete socket\n";
        return;
    }

    if(epoll_ctl(e_fd,EPOLL_CTL_DEL,fd,nullptr)){
        cout << "Error: can not delete socket\n";
        return;
    }

    for(int i =0 ; i < fds.size();++i){
        if(fds[i]==fd){
            fds.erase(fds.begin()+i);
        }
    }
}

boost::thread th;
void worker_accepter(int fd_listen);

void init(){
    socket_fd = socket(AF_INET,SOCK_STREAM,0);

    sockaddr_in addr;

    uint32_t port = 8080;
    addr.sin_port = htons(port);

    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_family = AF_INET;

    if(::bind(socket_fd,(sockaddr*)&addr,sizeof(addr))){
        cout << "Error can not bind addres\n";
        while(1){}
    }
    if(listen(socket_fd,10)){
        cout << "Error can not change socket mode\n";
        while(1){}
    }

    ep_fd = init_epoll();
    
    th = boost::thread(worker_accepter,socket_fd);

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
        cout << "Can not open file " << path << endl;
        return;
    }

    f.write(source.data(),source.size());
    f.close();
}

string get_file_section(string path,uint32_t& pos,uint32_t size){
    ifstream f(path,ios::binary|ios::ate);

    uint32_t file_end = f.tellg();
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
    ifstream fs(pat);

    fs.seekg(0,ios::end);

    if(!fs.is_open()){return 0;}

    uint32_t s = fs.tellg();
    fs.close();
    return s;
}

#define MAX_PCKG_SIZE 4096
#define END_PACKAGE 100
#define MIDDLE_PACKAGE 101
class DataPackage{
public:
    vector<Request> pckg;
    int id,next_id;

    uint32_t size1,ind;
    string source,p;

    DataPackage(){}
    DataPackage(string path,int id1){
        //cout << "PATH: " << path << endl;
        p = path;

        id = id1;
        ind = 0;

        next_id = -1;       

        size1 = GetSize(path.data());

        /*int count = 0;
        string source = read_file(path.data()); 
        int size = source.size();
        //source.resize(0);

        while(size-MAX_PCKG_SIZE>0){
            Request req(id,count,MIDDLE_PACKAGE);

            req.p.append(source.data()+(count*MAX_PCKG_SIZE),MAX_PCKG_SIZE);
            pckg.push_back(req);
            size -= MAX_PCKG_SIZE;
            count++;
        }

        Request req(id,count,END_PACKAGE);
        req.p.append(source.data()+(count*MAX_PCKG_SIZE),size);

        pckg.push_back(req);*/
    }
    DataPackage(vector<Request> req){
        for(int i = req.size()-1;i>=0;--i){
            source += req[i].p;
        }
        size1 = source.size();
    }
    Request next(){
        string src = get_file_section(p,ind,MAX_PCKG_SIZE);

        Request r(1,1,1);
        r.p = src;

        if(src.size()<=1){
            Request r(403,403,403);
            r.p = "ERROR: " + p;

            return r;
        }
        return r;
    }
    Request Dnext(){ 
        Request r;

        if(ind>=pckg.size()){
            r =Request(404,404,404);
            r.p = "ERROR\n";
        }else{
            r = pckg[ind++];
        }
        
        return r;
    }
};
boost::mutex mt;

bool accepted = false;
int a_fd = -1;

void worker_accepter(int fd_listen){
    mt.lock();
    accepted = false;
    a_fd = -1;
    mt.unlock();

    a_fd = accept(fd_listen,NULL,NULL);

    mt.lock();
    accepted = true;
    mt.unlock();
}
int accepter(){
    if(accepted){
        th.join();

        int new_fd = a_fd;
        th = boost::thread(worker_accepter,socket_fd);

        return new_fd;
    }else{
        return -1;
    }
}

string dirs(string path){
    string buffer;

    try{
        for(auto it : boost::filesystem::recursive_directory_iterator(path)){
            buffer += it.path().string()+"\n";
        }
    }catch(boost::filesystem::filesystem_error e){
        cout << "Error: " << e.what() << endl;
    }

    cout << buffer  << " end \n";
    return buffer;
}
vector<string> dirs_PS(string path){
    vector<string> buffer;

    try{
        for(auto it : boost::filesystem::recursive_directory_iterator(path)){
            if(it.is_directory()){
                continue;
            }
            buffer.push_back(it.path().string()+"\n");
        }
    }catch(boost::filesystem::filesystem_error e){
        cout << "Error: " << e.what() << endl;
    }
    return buffer;
}

map<int,DataPackage> pckgs;
int ids = 0;

map<int,Request> UP_req;
int up_ids = 0;

bool check_req(Request req){
    auto it = UP_req.find(req.id);

    if(it!=UP_req.end()&&req.pass==(*it).second.pass){
        return true;
    }
    return false;
}
void add_UP_req(Request r){
    if(check_req(r)){return;}

    UP_req[r.id] = r;
}
void delete_UP_req(Request r){
    if(!check_req(r)){return;}

    auto it = UP_req.find(r.id);
    UP_req.erase(it);
}
bool check_id(int id){
    auto it = pckgs.find(id);

    if(it!=pckgs.end()){
        return true;
    }
    return false;
}
void add_package_to_cahce(DataPackage pckg){
    if(check_id(pckg.id)){return;}

    pckgs[pckg.id] = pckg;
}
DataPackage GetPackage(int id){
    if(!check_id(id)){
        return DataPackage();
    }
    return pckgs[id];
}
void DeletePackage(int id){
    if(check_id(id)){
        auto it = pckgs.find(id);
        pckgs.erase(id);
    }
}
Request next_req_pckg(int id){
    if(!check_id(id)){
        Request r(404,404,404);
        r.p="Not founded req\n";
        return r;
    }

    return pckgs[id].next();
}
void init_dir_package(string path,int fd){
    cout << "User want downoload folder " << path << endl;
    boost::filesystem::directory_iterator iter(path);

    Request response(++ids,0,NEXT_FILE);
        
    DataPackage pckg(path,ids);
    
    Request req;
    for(auto it : iter){
        req = Request(ids,(it.is_directory()?DIR:0),0);
        req.p = it.path().string();

        pckg.pckg.push_back(req);
    }
    pckg.pckg[pckg.pckg.size()-1].type = END_DIR;

    cout << "Path: " << response.p << " type " << response.type << " id " << response.id<< " pass "<< response.pass << endl; 
    for(int i = 0; i < pckg.pckg.size();++i){
        cout << "Path: " << pckg.pckg[i].p << " type " << pckg.pckg[i].type << " id " << pckg.pckg[i].id << " pass "<< pckg.pckg[i].pass << endl; 
    }

    Psend(fd,response.convert());
    add_package_to_cahce(pckg);
}
void init_package(int fd,string path){
    /*if(read_file(path.data())==" "){
        cout << "Error\n";

        Request req(404,404,404);
        req.p = "File not founded\n";
        string msg = req.convert();

        Psend(fd,msg);
        delete_socket(fd,ep_fd);
    }*/

    int pckg_id = ++ids;
    add_package_to_cahce(DataPackage(path,pckg_id));

    Request r(pckg_id,0,NEXT);
    appU32(r.p,GetPackage(pckg_id).size1);

    cout << "File size in bytes: " << GetPackage(pckg_id).size1 << endl;
    cout.flush();

    string msg = r.convert();
    Psend(fd,msg);
}
void Requester(Request req,int fid){
    if(req.type==GET_FILE_LIST){
        cout << "User want get data list" << endl;
        cout.flush();

        Psend(fid,dirs("/home/cpp/FileServer/BASE" + req.p));      
    }else if(req.type==DOWNOLOAD_DATA){
        cout << "User want download data " << req.p << endl;
        init_package(fid,req.p);
    }else if(req.type == DOWNOLOAD_DIR){
        init_dir_package(req.p,fid);
    }
    else if(req.type == NEXT){
        Request r = next_req_pckg(req.id);

        if(r.type == 403){
            DeletePackage(req.id);
        }

        //cout << "pack n " << pckgs[req.id].ind << "/" << pckgs[req.id].pckg.size() << " sended" << endl;

        string msg = r.convert();
        Psend(fid,msg);
    }else if(req.type == NEXT_FILE){
        cout << "User ready for get a new file\n";

        if(!check_id(req.id)){
            Request rw(404,404,404);
            rw.p = "Package deleted\n";

            Psend(fid,rw.convert());
        }

        Request r(1,1,1);
        string msg;
        
        cout << "CALL\n";
        r = pckgs[req.id].Dnext();

        if(r.type == END_DIR){
            DeletePackage(req.id);
        }

        r.type = 1;
        msg = r.convert();
        Psend(fid,msg);
    }else if(req.type == CLOSE_CONNECTION){
        delete_socket(fid,ep_fd);
    }else if(req.type == REGISTRATE_UPLOAD_REQ){
        Request response(++up_ids,req.pass,UPLOAD_DATA);
        response.p = req.p;
        
        add_UP_req(response);

        string msg = response.convert();
        Psend(fid,msg);

        cout << "Upload registrated " << response.id << " path " << UP_req[response.id].p << endl;
    }else if(req.type == UPLOAD_DATA){
        if(req.p.size()>=1024U*1024U*1024U){
            Request req(404,404,404);
            req.p = "Package size overload\n";
            Psend(fid,req.convert());

            return;
        }if(!check_req(req)){
            Request req(404,404,404);
            req.p = "Not corretly request\n";
            Psend(fid,req.convert());

            return;
        }

        AddToFile(UP_req[req.id].p,req.p);

        Request r(1,1,1);
        Psend(fid,r.convert());
    }else if(req.type == DIR){
        boost::filesystem::create_directory(req.p);

        Request r(1,1,1);
        Psend(fid,r.convert());
    }
    else{
        cout <<"Error: unditifed command " << req.type << endl;
        Request r(404,404,404);
        r.p = "Unditifed command\n";
        string msg = r.convert();
        Psend(fid,msg); 
    }
}

void handler(){
    while(1){
        int fd_conn = accepter();

        if(fd_conn!=-1){
            cout << "Client connected\n";
            add_socket(fd_conn,ep_fd);
        }

        int ev_size = 20;
        vector<epoll_event> evs(ev_size);

        int size2 = epoll_wait(ep_fd,evs.data(),ev_size,1);

        for(int i = 0 ; i < size2;++i){
            int fd1 = evs[i].data.fd;

            if(evs[i].events&EPOLLHUP){
                cout << "Socket deleted " << endl;
                //cout.flush();
                delete_socket(fd1,ep_fd);
                continue;
            }

            //cout << "wait buffer status: "<< fd1 <<  "...";
            string m = Pread(fd1);
            //cout << "available\n";

            Request r(m); 
            Requester(r,fd1);
        }
    }
}

void free(){
    for(int i = 0 ; i < fds.size();++i){
        delete_socket(fds[i],ep_fd);
    }
}

int main(){
    //DataPackage pckg("/home/cpp/FileServer/data/hell.txt",2);

    init();
    cout << "Server init\n";
    handler();

    free();
    close(socket_fd);
    close(ep_fd);

    return 0;
}