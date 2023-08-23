#include <map>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

//Alex's includes

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;

//Alex's usings



#define FILECHUNKBUFSIZE 4096

//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the 'dfs-service.proto' file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in 'proto-src/dfs-service.grpc.pb.h' file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the 'override' directive as well. For example,
// if you have a service method named MyCoolMethod that takes a MyCoolMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyCoolMethod(ServerContext* context,
//                      const MyCoolMessageType* request,
//                      ServerWriter<MyCoolSegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //
    //May need to add override

    Status fileUploadRequest(ServerContext* context, ServerReader<dfs_service::UploadRequest>* sreader, dfs_service::UploadResponse* fileUploadRespond) override{
        if(context->IsCancelled()){
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded ir Client cancelled, prematurely ending request");
        }
        
        //Msg to populate each stream read
        dfs_service::UploadRequest FileUploadRequest;
        
        //Bytes read from upload
        size_t bytesRead = 0;

        //Reading first msg into FileUploadRequest
        sreader->Read(&FileUploadRequest);

        //Grab file name and create variable to read the file contents
        std::string FileName = FileUploadRequest.filename();
        std::string FilePath = WrapPath(FileName);
        size_t fileSize = FileUploadRequest.filesize();
        size_t fileChunkSize = FileUploadRequest.filechunksize();
        bytesRead += FileUploadRequest.mutable_filechunk()->length();
        dfs_log(LL_SYSINFO) << "-----------------------------------------------------------------";
        dfs_log(LL_SYSINFO) << "ServerSide | Accepting Client Request to store file: " << FileName; 

        //Copy File bytes into a string
        const std::string chunkContents = FileUploadRequest.filechunk();

        //Setting Response message variables
        fileUploadRespond->set_filename(FileName);
        fileUploadRespond->set_filestatus(0);

        //Mostly to keep track of whats going on
        struct stat fileStat;
        if(stat(FilePath.c_str(), &fileStat) != 0){
            dfs_log(LL_SYSINFO) << "ServerSide | Given file not found in system will be creating the file " << FileName;
        } 
        else{
            dfs_log(LL_SYSINFO) << "ServerSide | Given file found in system will be over writing file " << FileName;
        }

        //Opening the file to write to
        std::ofstream file;
        file.open(FilePath, std::ios::out | std::ios::trunc);

        //Copying the first section
        file << chunkContents;
        dfs_log(LL_SYSINFO) << "ServerSide | Bytes Download from Client: " << bytesRead << "/" << fileSize;
        if(fileSize > FILECHUNKBUFSIZE){
            while(sreader->Read(&FileUploadRequest)){
                //Break loop if all bytes are read
                if(bytesRead >= fileSize){
                    dfs_log(LL_SYSINFO) << "ServerSide | Transfer has been completed for file: " << FileName;
                    break;
                }
                const std::string chunkContents = FileUploadRequest.filechunk();
                bytesRead += FileUploadRequest.mutable_filechunk()->length();
                file << chunkContents;
                dfs_log(LL_SYSINFO) << "ServerSide | Bytes Download from Client: " << bytesRead << "/" << fileSize;
            }
        }
        file.close();

        fileUploadRespond->set_filestatus(2);

        dfs_log(LL_SYSINFO) << "ServerSide | Completed Client Request to store file: " << FileName;
        
        return Status::OK;
    }

    Status fileFetcher(ServerContext* context, const dfs_service::FetchRequest* fRequestMsg, ServerWriter<dfs_service::FetchResponse> *swriter) override{
        //If Query is no longer needed
        if(context->IsCancelled()){
            dfs_log(LL_ERROR) << "ServerSide | DEADLINE_EXCEEDED for file: " << fRequestMsg;
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded ir Client cancelled, prematurely ending request");
        }

        //Creating filePath string
        std::string fileName = fRequestMsg->filename();
        const std::string& filePath = WrapPath(fileName);
        
        dfs_log(LL_SYSINFO) << "-----------------------------------------------------------------";
        dfs_log(LL_SYSINFO) << "ServerSide | Accepting Client Request to fetch file: " << fileName; 

        //Mostly to keep track of whats going on
        struct stat fileStat;
        if(stat(filePath.c_str(), &fileStat) != 0){
            dfs_log(LL_ERROR) << "ServerSide | The requested file does not exist in the system: " << fileName;
            return Status(StatusCode::NOT_FOUND, "Requested Fetch not found on server"); //TO DO needs to be not found
        }
        else{
            dfs_log(LL_SYSINFO) << "ServerSide | Given file found in system: " << fileName;
        }

        //Create file size, vector or array to hold chunks of data
        off_t fileSize = fileStat.st_size;
        std::vector<char> fileChunk(FILECHUNKBUFSIZE, 0);

        //Creating a fResponseMsg element as it will be streamed
        dfs_service::FetchResponse fResponseMsg;
        fResponseMsg.set_filesize(fileSize);

        //Opening file in read mode
        std::ifstream file;
        file.open(filePath, std::ios::in);

        //Keep track of how many bytes are read
        std::size_t bytesRead = 0;
        while(!file.eof())
        {
            //Break loop if transfer is complete
            if(bytesRead >= fileSize){
                dfs_log(LL_SYSINFO) << "ServerSide | Completed uploading to client file: " << fileName;
                break;
            }

            //Add the check amount of data depending on bytes remaining and chunk size
            if(bytesRead + fileChunk.size() > fileSize){
                file.read(fileChunk.data(), fileSize-bytesRead);
                fResponseMsg.set_content(fileChunk.data(), file.gcount());
                bytesRead += file.gcount();
            }
            else{
                file.read(fileChunk.data(), fileChunk.size());
                fResponseMsg.set_content(fileChunk.data(), file.gcount());
                bytesRead += file.gcount();

            }

            //Stream the updated values
            dfs_log(LL_SYSINFO) << "ServerSide | Bytes uploaded Server to Client: " << bytesRead << "/" << fileSize;
            swriter->Write(fResponseMsg);
        }

        //If there was an issue with writing (streaming) msgs then  end the request
        if(bytesRead > fileSize){
            dfs_log(LL_ERROR) << "ERROR: ServerSide | Bytes read is not equal to fileSize [Bytes/fileSize]=[" << bytesRead << "/" << fileSize << "] for file: " << fileName;
            return Status(StatusCode::CANCELLED, "Data transfer issue");
        }
        

        return Status::OK;
    }

    Status fileDeleter(ServerContext* context, const dfs_service::DeleteRequest* dRequestMsg, ::google::protobuf::Empty* dResponseMsg) override{
        //If Query is no longer needed
        if(context->IsCancelled()){
            dfs_log(LL_ERROR) << "Deadline exceeded or Client cancelled request, prematurely ending request";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded ir Client cancelled, prematurely ending request");
        }
        
        //Creating filePath string
        std::string FileName = dRequestMsg->filename();
        const std::string& filePath = WrapPath(FileName);

        dfs_log(LL_SYSINFO) << "-----------------------------------------------------------------";
        dfs_log(LL_SYSINFO) << "ServerSide | Accepting Client Request to delete file: " << FileName; 

        //Mostly to keep track of whats going on
        struct stat fileStat;
        if(stat(filePath.c_str(), &fileStat) != 0){
            dfs_log(LL_ERROR) << "The requested file does not exist in the system: " << FileName;
            return Status(StatusCode::NOT_FOUND, "File was not found on server");
        } 

        //Trying to delete the file
        int TryDelFile = remove(filePath.c_str());
        if(TryDelFile != 0){
            dfs_log(LL_ERROR) << "Server unable to delete file: " << FileName;
            return Status(StatusCode::CANCELLED, "Server was unable to delete file on system");
        }

        dfs_log(LL_SYSINFO) << "ServerSide | Completed Client Request to delete file: " << FileName; 

        return Status(StatusCode::OK, "File Deleted");
    }

    Status fileLister(ServerContext* context, const ::google::protobuf::Empty* request, dfs_service::ListResponse* filesList) override{
        //If Query is no longer needed
        if(context->IsCancelled()){
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded ir Client cancelled, prematurely ending request");
        }
        
        dfs_log(LL_SYSINFO) << "-----------------------------------------------------------------";
        dfs_log(LL_SYSINFO) << "ServerSide | Accepting Client Request to send a list of files in directory"; 

        //Create directory path to look in. Recreated variable incase I needed to edit the string
        std::string directoryPath = mount_path;

        //Creating Directory to look ing
        DIR *dr;
        struct dirent *en;
        dr = opendir(directoryPath.c_str());

        //Stat is needed to decide if it's a file or path
        struct stat FileOrDirectory;

        //Loop through files and paths in current directory
        while((en = readdir(dr)) != NULL){
            std::string FileName = en->d_name;
            std::string CurrentPathNFile = directoryPath+FileName;

            //Send file if it's a file and not a path
            if(stat(CurrentPathNFile.c_str(), &FileOrDirectory) == 0 && FileOrDirectory.st_mode & S_IFREG){
                dfs_service::ListElementResponse* FileInfo = filesList->add_file();
                FileInfo->set_filename(FileName);

                //Getting time file was last modified
                auto mtime = FileOrDirectory.st_mtim;
                FileInfo->mutable_mtime()->set_seconds(mtime.tv_sec);
                dfs_log(LL_SYSINFO) << "ServerSide | Found File: " << FileName << " and timestamp: " << FileInfo->mutable_mtime()->seconds(); 
            }
        }
        //Freeing memory 
        closedir(dr);


        dfs_log(LL_SYSINFO) << "ServerSide | Completed Client Request to send a list of files in directory"; 
        
        return Status::OK;
    }

    Status fileStatuser(ServerContext* context, const dfs_service::StatusRequest* sRequestMsg, dfs_service::StatusResponse* sResponseMsg) override{
        //If Query is no longer needed
        if(context->IsCancelled()){
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded ir Client cancelled, prematurely ending request");
        }

        //Creating filePath string
        std::string FileName = sRequestMsg->filename();
        const std::string& filePath = WrapPath(FileName);
        sResponseMsg->set_filename(FileName);

        dfs_log(LL_SYSINFO) << "-----------------------------------------------------------------";
        dfs_log(LL_SYSINFO) << "ServerSide | Accepting Client Request to send status of file: " << FileName;

        //Mostly to keep track of whats going on
        struct stat fileStat;
        if(stat(filePath.c_str(), &fileStat) != 0){
            dfs_log(LL_ERROR) << "ServerSide | The requested file does not exist in the system: " << FileName;
            sResponseMsg->set_filesize(0);
            sResponseMsg->set_filestatus(-1);
            return Status(StatusCode::NOT_FOUND, "Requested Status not found on server"); //TO DO needs to be not found
        }

        //Grabbing when file was last modified
        auto mtime = fileStat.st_mtim;
        sResponseMsg->mutable_mtime()->set_seconds(mtime.tv_sec);

        //Setting remainder info       
        sResponseMsg->set_filesize(fileStat.st_size);
        sResponseMsg->set_filestatus(1);

        dfs_log(LL_SYSINFO) << "ServerSide | Completed Client Request to send status of file: " << FileName;

        return Status::OK;
    }


};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode exiting";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    ServerBuilder builder;
    DFSServiceImpl service(this->mount_path);

    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server running on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//