#include <iostream>
#include <fstream>
#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <chrono>
#include "Networker.h"
#include <boost/process.hpp>

using namespace std;
using namespace boost;
using namespace asio;

asio::io_context cnt;
ip::tcp::socket skc(cnt);
ip::tcp::endpoint ep(ip::make_address_v4("127.0.0.1"),8080);

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
//Pack tar -czf temp.rar -C path+BASE .
//Unpack tar -xzf temp.rar -C source_path+GetName(dst) .

void run_cmd(string cmd){
    boost::process::v2::shell sh(cmd);
    boost::process::v2::process pr(cnt,sh.exe(),sh.args());

    pr.wait();
}

void Pack(string src,string dst){
    string cmd = "tar -czf " + dst +".rar" + " -C "+ src;
    cout << cmd << endl;
    cout.flush();

    boost::process::v2::shell sh(cmd);
    boost::process::v2::process pr{cnt,sh.exe(),sh.args()};
    pr.wait();
}
void Unpack(string src,string dst){
    string cmd = "tar -xzf "+dst+".rar"+ " -C "+ src;
    cout << cmd << endl;
    cout.flush();

    boost::process::v2::shell sh(cmd);
    boost::process::v2::process pr{cnt,sh.exe(),sh.args()};
    pr.wait();
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

boost::thread thr;
boost::mutex mt;

char anim[] = {'|','/','-','\\','/','-','\\'};
uint64_t ind = 0;

boost::atomic<int> per = 0;
boost::atomic<uint32_t> last_up = 0; 
boost::atomic<bool> f = false;

void animation(){
    while(f){
        cout << "\r";
        cout << anim[ind%6] << " %" << per;
        cout.flush();

        boost::this_thread::sleep_for(boost::chrono::milliseconds(400));
        ++ind;
    }
}

void start(){
    per = 0;
    ind = 0;
    if(f){
        return;
    }
    f = true;
    thr = boost::thread(animation);
}
void stop(){
    if(!f){
        return;
    }
    f = false;
    thr.join();
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

void RequestAnalaizer(const Request& r){
    cout << "Type " << r.type << " pass " << r.pass << " id " << r.id << endl;
    //cout << "Params " << r.p << endl;
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

void Rsend(boost::asio::ip::tcp::socket& s,Request r){
    string msg = r.convert();
    B_send(s,msg);
}
Request Rread(boost::asio::ip::tcp::socket& s){
    string msg = B_read(s);
    return Request(msg);
}

void downoload_file(string src){
    string dst = "temp.rar";

    Request registrated_req(0,0,REG_DOWNOLOAD_REQ);
    registrated_req.p = src;

    Rsend(skc,registrated_req);
    registrated_req = Rread(skc);

    string cnvrt = registrated_req.convert();
    uint32_t size = registrated_req.pass;

    Request server_response;

    cout << "Dwnoloading..." << endl;
    start();
    do{
        Rsend(skc,cnvrt);
        string m = B_read(skc);
        server_response = Request(m);

        if(server_response.type == NOT_FONDED){
            cout << "Server response: " << server_response.p << endl;
            cout << "File not downoloaded\n";
            break;
        }
        AddToFile(dst,server_response.p);

        cout << "Recv\n";
        per = ceil(float(float(GetSize(dst.data()))/float(size))*100);
    }while(server_response.type==1);
    stop();
    cout << "Done" << endl;

    string sr = source_dir+GetName(registrated_req.p);

    Unpack(sr,DeleteExt(dst));
    run_cmd("rm -r temp.rar");
}   
void upload(string src){
    cout << "Packing package...";
    Pack(DeleteName(src)+" "+GetName(src),"temp");    
    cout << "Done" << endl;

    Request reg_upload_req(0,0,REG_UPLOAD_REQ);
    reg_upload_req.p = src;

    Rsend(skc,reg_upload_req);

    reg_upload_req = Rread(skc);
    Request serv_response;

    cout << "Uploading...\n";
    uint32_t pos = 0;int si = GetSize("temp.rar");
    start();

    do{
        serv_response = reg_upload_req;
        serv_response.p = get_file_section("temp.rar",pos,MAX_PCKG_SIZE);

        Rsend(skc,serv_response);

        if(serv_response.p.size()!=MAX_PCKG_SIZE){
            break;
        }

        serv_response = Rread(skc);        
        per = int(ceilf(float(float(pos)/float(si))*100.f));
    }while(serv_response.type == 1); 
    stop();
    cout << "Done\n";

    serv_response = reg_upload_req;
    serv_response.type = END_FILE;
    serv_response.p = GetName(src);//(boost::filesystem::is_directory(src)?GteName(src):GetName(src));
    Rsend(skc,serv_response);
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
            cout << "DOFO src - downoload folder; src - folder path;\n";
            cout << "DOFI src dst - downoload file; src - your file; dst - destination file\n";
        }else if(m=="FLI"){
            cin >> p1;
            GetFilesList(p1);
        }else if(m=="FLIC"){
            cin >> p1;
            dirs(source_dir+p1);
        }
        else if(m=="DOFI"){
            cin >> p1;
            downoload_file(p1);
        }else if(m=="UPFI"){
            cin >> p1;

            upload(p1);
        }else{
            cout << "Unditifed command\n";
        }

    }

    close_connection();
    return 0;
}
