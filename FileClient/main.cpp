#include <iostream>
#include <fstream>
#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <chrono>
#include "Networker.h"

using namespace std;
using namespace boost;
using namespace asio;

asio::io_context cnt;
ip::tcp::socket skc(cnt);
ip::tcp::endpoint ep(ip::make_address_v4("127.0.0.1"),8080);

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
        cout << "Error: can nt get file size\n";
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

uint32_t GetSize(const char* pat){
    ifstream fs(pat,ios::binary);

    fs.seekg(0,ios::end);

    if(!fs.is_open()){return 0;}

    uint32_t s = fs.tellg();
    fs.close();
    return s;
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
void reconnect(){
    skc.connect(ep);
}

boost::thread th;
boost::mutex mt;

char anim[] = {'|','/','-','\\','/','-','\\'};
uint64_t ind = 0;

boost::atomic<int> per = 0;
boost::atomic<uint32_t> last_up = 0; 
boost::atomic<bool> f = false;

void animation(){
    while(f){
        cout << "\r";
        cout << anim[ind%7] << " %" << per;
        cout.flush();

        boost::this_thread::sleep_for(boost::chrono::milliseconds(300));
        ++ind;
    }
}

void start(){
    per = 0;
    if(f){
        return;
    }
    f = true;
    th = boost::thread(animation);
}
void stop(){
    if(!f){
        return;
    }
    f = false;
    th.join();
    cout << '\r';
}

string source_path = "downoload_files/";

#define DOWNOLOAD_DATA 1
#define UPLOAD_DATA 2
#define DELETE_SESSIA 3
#define GET_FILE_LIST 4
#define NEXT 5
#define NEXT_FILE 7
#define CLOSE_CONNECTION 6
#define DOWNOLOAD_DIR 10
#define DIR 8
#define END_DIR 9
#define REGISTRATE_UPLOAD_REQ 11

#define MAX_PCKG_SIZE 4096
#define END_PACKAGE 100
#define MIDDLE_PACKAGE 101
#define MAX_LOAD_SIZE 4096

class DataPackage{
public:
    int id;
    uint32_t size,ind;

    string name;
    string source;
    vector<Request> pckg;

    DataPackage(){}
    DataPackage(string path,int id1){
        id = id1;
        ind = 0;

        int count = 0;

        string source = read_file(path.data());
        int size = source.size();

        while(size-MAX_PCKG_SIZE>0){
            Request req(id,count,MIDDLE_PACKAGE);

            req.p.append(source.data()+(count*MAX_PCKG_SIZE),MAX_PCKG_SIZE);
            pckg.push_back(req);
            size -= MAX_PCKG_SIZE;
            count++;
        }

        Request req(id,count,END_PACKAGE);
        req.p.append(source.data()+(count*MAX_PCKG_SIZE),size);

        pckg.push_back(req);
    }
    DataPackage(vector<Request> req){
        for(int i =0; i < req.size();++i){
            source+=req[i].p;
        }

        size = source.size();
    }
    Request next(){
        if(ind>=pckg.size()){
            return Request(404,404,404);
        }
        return pckg[ind++];
    }
};
void GetFilesList(){
    Request req(1,1,4);
    string  msg;

    cin >> msg;
    req.p = msg;

    msg = req.convert();
    B_send(skc,msg);
    
    msg = B_read(skc);

    cout << msg << endl;
}
void GetFilesList(string path){
    Request req(1,1,4);

    req.p = path;

    path = req.convert();
    B_send(skc,path);
    
    path = B_read(skc);

    cout << path << endl;
}
string GetName(string path){
    string name = "";
    for(int i = path.size()-1;i>=0;--i){
        if(path[i]=='\\'||path[i]=='/'){
            name.append(path.begin()+i+1,path.end());
            return name;
        }
    }
    return name;
}

string BytesToMK(uint32_t bytes){    
    unordered_map<int,string> mp{
        {1024,"KB"},
        {1024*1024,"MB"}
    };

    uint32_t s = 1024*1024;
    if(bytes%s==0){
        return to_string(bytes/s)+ " MB";
    }
    return to_string(bytes/1024)+ "KB " + to_string(bytes%1024) + "B ";
}

string source_dir = "/home/cpp/FileClient/downoload_files/";
string dir = "";

void create_directory(string path){
    boost::filesystem::create_directory(boost::filesystem::path(source_dir+path));
}

void AddToFile(string path,string source){
    ofstream f(path,ios::app);

    if(!f.is_open()){
        cout << "Can not open file " << path << endl;
        return;
    }
    f.write(source.data(),source.size());
    f.close();
}

void downoload_file(string dst,string src){
    Request req(1,1,1);

    req.p = src;

    string converted = req.convert();
    B_send(skc,converted);

    converted = B_read(skc);
    req = Request(converted);
    uint32_t p = 0;
    uint32_t segments = readU32(p,req.p);

    req.p = "";
    converted = req.convert();

    cout << BytesToMK(segments) << endl;

    Request r(1,1,1);

    start();
    while(r.type == 1){
        B_send(skc,converted);

        string m = B_read(skc);
        r = Request(m);

        if(r.type!=1){
            //reconnect();
            break;
        }

        AddToFile(dst,r.p);
        
        per = ceil(float(float(GetSize(dst.data()))/float(segments))*100.f);
    }
    stop();
}
void downoload_folder(string dst,string src){
    string d = dst+"/"+GetName(src);
    cout << "Current dir " << d << endl;
    boost::filesystem::create_directory(d);

    Request req(0,0,DOWNOLOAD_DIR);
    req.p = src;

    string msg = req.convert(),source;
    B_send(skc,msg);

    msg = B_read(skc);
    req = Request(msg);

    source = req.convert();
    Request r;

    do{
        B_send(skc,source);

        msg = B_read(skc);
        r = Request(msg);

        if(r.type==404){break;}

        if(r.pass == DIR){
            cout << "Sub dir detected " << d+"/"+GetName(r.p) << endl;
            downoload_folder(d,r.p);
        }else{
            cout << "Downoload file src " << r.p << " dst " << d+"/"+GetName(r.p) << endl;
            downoload_file(d+"/"+GetName(r.p),r.p);
        }      
    }while(r.type==1);
}
void upload_file(string dst,string src){
    Request req(0,0,REGISTRATE_UPLOAD_REQ);
    req.pass = skc.native_handle();
    req.p = dst;

    cout << "DST " << dst << " SRC " << src << endl;

    string msg = req.convert();
    B_send(skc,msg);

    msg = B_read(skc);
    Request r = req = Request(msg);

    string buffer;uint32_t i =0;uint32_t file_size = GetSize(src.data());

    start();

    while(1){
        buffer = get_file_section(src,i,MAX_PCKG_SIZE);

        r = req;
        r.p = buffer;

        B_send(skc,r.convert());

        msg = B_read(skc);
        r = Request(msg);
        if(r.type!=1){
            cout<< "Type " << r.type << " source " << r.p << endl;
            break;
        }
        if(buffer.size()<MAX_PCKG_SIZE){
            break;
        }
        per = ceil((float(i)/float(file_size))*100);
    } 

    stop();

}
void upload_folder(string dst,string src_dir){
    Request r(DIR,DIR,DIR);
    r.p = dst+GetName(src_dir);

    B_send(skc,r.convert());
    r = Request(B_read(skc));

    for(auto it: boost::filesystem::directory_iterator(src_dir)){
        if(it.is_directory()){
            string sub_dir = dst+"/"+GetName(it.path().string())+"/";
            cout << "Sub dir " << sub_dir << endl;

            string load_dir = it.path().string()+"/";
            cout << "Upload dir " << load_dir << endl;

            upload_folder(sub_dir,load_dir);
            continue;
        }
        cout << "Upload file " << it.path().string() << endl;
        upload_file(dst+"/"+GetName(it.path().string()),it.path().string());
    }
}

void close_connection(){
    Request req(0,0,CLOSE_CONNECTION);

    string msg = req.convert();
    B_send(skc,msg);

    skc.close();
}

void dirs(string path){
    for(auto it : boost::filesystem::recursive_directory_iterator(path)){
        cout << it.path() << endl;
        cout.flush();
    }
}

string terminate_op = "TER";

int main(){
    skc.connect(ep);
    
    cout << "HELP if you want get command list\n";
    cout.flush();

    while(1){

        string m,p1,p2;
        cout << "Write a command: ";
        cin >> m;

        if(m=="HELP"){
            cout << "TER - terminate this operation\n";
            cout << "FLI path - print file list on server\n";
            cout << "FLIC path - print file list on yout computer in src dir\n";
            cout << "UPFO src dst - upload folder; src - folder path; dst - destination\n";
            cout << "UPFI src dst - upload file; src - your file; dst - destination file\n";
            cout << "DOFO src dst - downoload folder; src - folder path; dst - destination\n";
            cout << "DOFI src dst - downoload file; src - your file; dst - destination file\n";
        }else if(m=="FLI"){
            cin >> p1;

            if(p1==terminate_op){
                continue;
            }

            GetFilesList(p1);
        }else if(m=="FLIC"){
            cin >> p1;

            if(p1==terminate_op){
                continue;
            }

            dirs(source_dir+p1);
        }
        else if(m=="UPFO"){
            cin >> p1>>p2;

            if(p1==terminate_op||p2==terminate_op){
                continue;
            }

            upload_folder(p1,p2);
        }else if(m =="UPFI"){            
            cin >> p1>> p2;

            if(p1==terminate_op||p2==terminate_op){
                continue;
            }

            upload_file(p1,p2);
        }else if(m=="DOFI"){
            cin >> p1>>p2;

            if(p1==terminate_op||p2==terminate_op){
                continue;
            }

            downoload_file(p1,p2);
        }else if(m=="DOFO"){
            cin >> p1 >> p2;

            if(p1==terminate_op){
                continue;
            }

            downoload_folder(p1,p2);
        }else{
            cout << "unditifed command\n";
        }

    }

    close_connection();
    return 0;
}
