#include <map>
#include <mutex>
#include <shared_mutex>
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

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;

#define FILECHUNKBUFSIZE 4096

//Writer lock for each file and which client has it
typedef struct fileMutexInfo{
    std::mutex FileMutex;
    std::string ClientID;
}fileMutexInfo;


using FileRequestType = dfs_service::CBLRequest;
using FileListResponseType = dfs_service::CBLResponse;

extern dfs_log_level_e DFS_LOG_LEVEL;


class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;


    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;
    
    
    //////////////////////////////////////////////////////
    //This section writer lock information for each file//
    //I pray these don't have deadlocks                 //
    //////////////////////////////////////////////////////
    //MasterLock and conditional allows for the update of writer locks
    std::condition_variable MasterLock_CV;
    bool MasterLock_Avail = true;
    std::mutex MasterMutex;
    std::map<std::string, fileMutexInfo> fileMutexes;
    
    //Requests to get file mutex
    bool fileMutex_Request(std::string FileName, std::string clientID){
        dfs_log(LL_SYSINFO) << "ServerSide | Client [" << clientID << "] is requesting mutex for File " << FileName;
        
        //Acquire the lock & critical section
        std::unique_lock<std::mutex> MasterLock (MasterMutex);
        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] attempting to get master lock";
        while(!MasterLock_Avail){MasterLock_CV.wait(MasterLock);}
        MasterLock_Avail = false;
        MasterLock.unlock();
        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] has master lock";

        bool SuccessfulRequest;
        //If then to check if mutex has been created and is in the fileMutexes
        dfs_log(LL_SYSINFO) << "ServerSide | Checking for file mutex for " << FileName;
        if(fileMutexes.find(FileName) != fileMutexes.end()){
            dfs_log(LL_SYSINFO) << "ServerSide | File Mutex exists in system";

            //Client already has the mutex
            if (fileMutexes[FileName].ClientID == clientID){
                dfs_log(LL_SYSINFO) << "ServerSide | File Mutex is already assigned to the Client performing this request ClientID: " << fileMutexes[FileName].ClientID;
                SuccessfulRequest = true;
            }
            //Check if a client has it or if it's available
            else if(fileMutexes[FileName].ClientID != "NOT_IN_USE"){
                dfs_log(LL_SYSINFO) << "ServerSide | File Mutex is currently being used by a different Client: " << fileMutexes[FileName].ClientID;
                SuccessfulRequest = false;
            }
            //it's available and already created
            else{
                fileMutexes[FileName].FileMutex.lock();
                fileMutexes[FileName].ClientID =  clientID;
                dfs_log(LL_SYSINFO) << "Serverside | File Mutex available and is assigned now to Client: " << fileMutexes[FileName].ClientID;
                fileMutexes[FileName].FileMutex.unlock();
                SuccessfulRequest = true;
            }
        }
        //Mutex has not been created yet
        else{
            fileMutexes[FileName].FileMutex.lock();
            fileMutexes[FileName].ClientID = clientID;
            dfs_log(LL_SYSINFO) << "ServerSide | File Mutex has been created for file: " << FileName << " and is assigned now to Client: " << fileMutexes[FileName].ClientID;
            fileMutexes[FileName].FileMutex.unlock();
            SuccessfulRequest = true;
        }


        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] attempting to release master lock";
        MasterLock.lock();
        MasterLock_Avail = true;
        MasterLock.unlock();
        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] no longer has master lock";
        MasterLock_CV.notify_all();

        return SuccessfulRequest;

    }

    //Releaes lock but is still available if file exists
    bool fileMutex_Release(std::string FileName, std::string clientID){

        //Acquire the lock & critical section
        std::unique_lock<std::mutex> MasterLock (MasterMutex);
        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] attempting to get master lock";
        while(!MasterLock_Avail){MasterLock_CV.wait(MasterLock);}
        MasterLock_Avail = false;
        MasterLock.unlock();
        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] has master lock";


        dfs_log(LL_SYSINFO) << "ServerSide | Attempting to release File Mutex for file: " << FileName;
        bool SuccessfulRelease;

        //If then to check if mutex exist in the map of filenames and mutexes
        if(fileMutexes.find(FileName) != fileMutexes.end()){
            //Check the correct client is trying to perform the action
            if(fileMutexes[FileName].ClientID == clientID){
                fileMutexes[FileName].FileMutex.lock();
                fileMutexes[FileName].ClientID = "NOT_IN_USE";
                fileMutexes[FileName].FileMutex.unlock();
                dfs_log(LL_SYSINFO) << "ServerSide | File Mutex for file, " << FileName << ", has been released";
                SuccessfulRelease = true;
            } 
            //Incorrect client is releasing the lock
            else{       
                dfs_log(LL_ERROR) << "ServerSide | Mutex for file, " << FileName << ", has not been released. ClientID does not have ownership";
                SuccessfulRelease = false;
            }
        }
        //Mutex at this time does not exist
        else{
            dfs_log(LL_ERROR) << "ServerSide | Mutex for file, " << FileName << ", has not been released. It does not currently exist";
            SuccessfulRelease = false;
        }

        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] attempting to release master lock";
        MasterLock.lock();
        MasterLock_Avail = true;
        MasterLock.unlock();
        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] no longer has master lock";
        MasterLock_CV.notify_all();

        return SuccessfulRelease;
    }

    //Deletes the mutex if the file is deleted
    bool fileMutex_Delete(std::string FileName, std::string clientID){
        //Acquire the lock & critical section
        std::unique_lock<std::mutex> MasterLock (MasterMutex);
        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] attempting to get master lock";
        while(!MasterLock_Avail){MasterLock_CV.wait(MasterLock);}
        MasterLock_Avail = false;
        MasterLock.unlock();
        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] has master lock";

        dfs_log(LL_SYSINFO) << "ServerSide | Attempting to delete Mutex for file: " << FileName;
        bool SuccessfulDelete;
        //Check if mutex exist in current fileMutexes
        if(fileMutexes.find(FileName) != fileMutexes.end()){
            //Check the correct client is requesting this action
            if(fileMutexes[FileName].ClientID == clientID){
                dfs_log(LL_SYSINFO) << "ServerSide | File Mutex for file, " << FileName << ", has been deleted";
                fileMutexes.erase(FileName);
                SuccessfulDelete = true;
            }
            //Output incorrect client is requesting to delete the lock
            else{
                dfs_log(LL_ERROR) << "ServerSide | File Mutex for file, " << FileName << ", has not been deleted. ClientID does not have ownership";
                SuccessfulDelete = false;
            }     
        }
        //File Mutex delete when file mutex does not exist
        else{
            dfs_log(LL_ERROR) << "ServerSide | File Mutex for file, " << FileName << ", does not currently exist";
            SuccessfulDelete = false;
        }

        //Release the lock & critical section
        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] attempting to release master lock";
        MasterLock.lock();
        MasterLock_Avail = true;
        MasterLock.unlock();
        dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID << "] no longer has master lock";
        MasterLock_CV.notify_all();

        return SuccessfulDelete;
    }
    
    //Calls the correct function depending if the file exists in the system or not
    bool fileMutex_Release_Or_Delete(std::string FileName, std::string clientID, bool FileInSystem){
        if(fileMutex_IsClientOwnerCheck(FileName, clientID)){
            if(FileInSystem){
                dfs_log(LL_SYSINFO) << "ServerSide | Calling fileMutex_Release for file: " << FileName;
                return fileMutex_Release(FileName, clientID);
            }
            else{
                dfs_log(LL_SYSINFO) << "ServerSide | Calling fileMutex_Delete for file: " << FileName;
                return fileMutex_Delete(FileName, clientID);
            }
        }
        return false;
    }


    //Checks if current client is the owner
    bool fileMutex_IsClientOwnerCheck(std::string FileName, std::string clientID){
        //Check if filename mutex exists
        if(fileMutexes.find(FileName) != fileMutexes.end()){
            if(fileMutexes[FileName].ClientID == clientID){
                return true;
            }
        }
        dfs_log(LL_SYSINFO) << "Client [" << clientID << "] is not the owner of file mutex: " << FileName; 
        return false;
    }

    //Returns the owner of the mutex
    std::string fileMutex_getOwner(std::string FileName){
        if(fileMutexes.find(FileName) != fileMutexes.end()){
            return fileMutexes[FileName].ClientID;
        }
        return "Not Created";
    }


public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetAddress(server_address);
        this->runner.SetService(this);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);

    }


    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {

        dfs_log(LL_SYSINFO) << "ServerSide | Processing Callback";
        Status cblStatusMsg = CallbackList(context, request, response);
        dfs_log(LL_SYSINFO) << "ServerSide | Completed Callback";
        if(cblStatusMsg.error_code() != StatusCode::OK){
            dfs_log(LL_ERROR) << "ServerSide | Processing Callback failed. Error Message: " << cblStatusMsg.error_message();
        }
        else{
            dfs_log(LL_SYSINFO) << "ServerSide | Completed Callback";
        }



    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while(true) {

            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }


    Status fileUploadRequest(ServerContext* context, ServerReader<dfs_service::UploadRequest>* sreader, dfs_service::UploadResponse* fileUploadRespond) override{      
        //Msg to populate each stream read
        dfs_service::UploadRequest FileUploadRequest;
        
        //Bytes read from upload
        off_t bytesRead = 0;

        //Reading first msg into FileUploadRequest
        sreader->Read(&FileUploadRequest);

        //Grab file name and create variable to read the file contents
        std::string FileName = FileUploadRequest.filename();
        std::string FilePath = WrapPath(FileName);
        std::string ClientID = FileUploadRequest.clientid();
        uint32_t Client_CheckSum = FileUploadRequest.cfilechecksum();
        time_t client_mtime = FileUploadRequest.cfilemtime().seconds();
        off_t fileSize = FileUploadRequest.filesize();
        bytesRead += FileUploadRequest.filechunk().length();
        
        //Double checking the lock is correct
        if(!fileMutex_IsClientOwnerCheck(FileName, ClientID)){
            dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << ClientID <<  "] Request does not have lock to update stored file:" << FileName;
            dfs_log(LL_SYSINFO) << "ServerSide | Current Owner of file [" << FileName << "] is Client ID: [" << fileMutex_getOwner(FileName).c_str() << "]";
            return Status(StatusCode::CANCELLED, "Client should've had lock for file to peform this action");
        }
        
        dfs_log(LL_SYSINFO) << "ServerSide | Accepting Client Request to store file: " << FileName; 

        //Copy File bytes into a string
        const std::string chunkContents = FileUploadRequest.filechunk();

        //Setting Response message variables
        fileUploadRespond->set_filename(FileName);

        //Mostly to keep track of whats going on
        bool FileInSystem; //Delete == false | Release == true
        struct stat fileStat;
        if(stat(FilePath.c_str(), &fileStat) != 0){
            FileInSystem = false;
            dfs_log(LL_SYSINFO) << "ServerSide | Given file not found in system will be creating file: " << FileName;
        } 
        else{
            FileInSystem = true;
            dfs_log(LL_SYSINFO) << "ServerSide | Given file found in system will be over writing file: " << FileName;
        }

        //If file is in system compare the checksums and last modified times
        if(FileInSystem){
            uint32_t Server_Checksum = dfs_file_checksum(FilePath, &crc_table);
            if(Server_Checksum == Client_CheckSum){
                dfs_log(LL_ERROR) << "ServerSide | File is the same on server for file: " << FileName;
                fileMutex_Release_Or_Delete(FileName, ClientID, FileInSystem);
                return Status(StatusCode::ALREADY_EXISTS, "Already exists");
            }

            time_t server_mtime = fileStat.st_mtim.tv_sec;
            if(server_mtime >= client_mtime){
                dfs_log(LL_ERROR) << "ServerSide | Client File older than server file: " << FileName;
                fileMutex_Release_Or_Delete(FileName, ClientID, FileInSystem);
                return Status(StatusCode::CANCELLED, "File is newer or the same on server");
            } 
        }

        if(context->IsCancelled()){
            //Releasing lock
            dfs_log(LL_ERROR) << "ServerSide | Deadline exceeded for file: " << FileName;
            fileMutex_Release_Or_Delete(FileName, ClientID, FileInSystem);
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, prematurely ending request");
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
                //bytesRead += FileUploadRequest.mutable_filechunk()->length();
                bytesRead += FileUploadRequest.filechunk().length();
                file << chunkContents;
                dfs_log(LL_SYSINFO) << "ServerSide | Bytes Download from Client: " << bytesRead << "/" << fileSize;
            }
        }
        file.close();

        dfs_log(LL_SYSINFO) << "ServerSide | Completed Client Request to store file: " << FileName;

        //Releasing lock
        if(!fileMutex_Release(FileName, ClientID)){
            return Status(StatusCode::INTERNAL, "Error releasing lock");
        }
        
        return Status::OK;
    }

    Status fileFetcher(ServerContext* context, const dfs_service::FetchRequest* fRequestMsg, ServerWriter<dfs_service::FetchResponse> *swriter) override{
        //Creating filePath string and setting copyfile to false. Which is false until all prechecks are done
        //then it's set to true and the client will write a new file
        std::string fileName = fRequestMsg->filename();
        const std::string& filePath = WrapPath(fileName);
        dfs_service::FetchResponse fResponseMsg;
        fResponseMsg.set_copyfile(false);
        
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

        //If Query is no longer needed
        if(context->IsCancelled()){
            dfs_log(LL_ERROR) << "ServerSide | DEADLINE_EXCEEDED for file: " << fRequestMsg;
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded ir Client cancelled, prematurely ending request");
        }

        //Only perform the checksum and mtime compare is the file exists on the client
        if(fRequestMsg->clienthasfile()){
            uint32_t Client_Checksum = fRequestMsg->cfilechecksum();
            uint32_t Server_Checksum = dfs_file_checksum(filePath, &crc_table);
            if(Server_Checksum == Client_Checksum){
                dfs_log(LL_ERROR) << "ServerSide | File is the same on server for file: " << fileName;
                return Status(StatusCode::ALREADY_EXISTS, "Already exists");
            }

            time_t server_mtime = fileStat.st_mtim.tv_sec;
            time_t client_mtime = fRequestMsg->cfilemtime().seconds();
            if(server_mtime <= client_mtime){
                dfs_log(LL_ERROR) << "ServerSide | Client File newer than server file: " << fileName;
                return Status(StatusCode::CANCELLED, "File is newer or the same on server");
            } 
        }

        fResponseMsg.set_copyfile(true);
    


        //Create file size, vector or array to hold chunks of data
        off_t fileSize = fileStat.st_size;
        std::vector<char> fileChunk(FILECHUNKBUFSIZE, 0);

        //Setting filsize
        fResponseMsg.set_filesize(fileSize);

        //Opening file in read mode
        std::ifstream file;
        file.open(filePath, std::ios::in);

        //Keep track of how many bytes are read
        off_t bytesRead = 0;
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

        //Closing file for good practice
        file.close();

        //If there was an issue with writing (streaming) msgs then  end the request
        if(bytesRead > fileSize){
            dfs_log(LL_ERROR) << "ERROR: ServerSide | Bytes read is not equal to fileSize [Bytes/fileSize]=[" << bytesRead << "/" << fileSize << "] for file: " << fileName;
            return Status(StatusCode::CANCELLED, "Data transfer issue");
        }
        

        return Status::OK;
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
            sResponseMsg->set_fileexists(false);
            sResponseMsg->set_filesize(0);
            return Status(StatusCode::NOT_FOUND, "Requested Status not found on server"); //TO DO needs to be not found
        }

        //Setting remainder info 

        //Sets if the file was found in directory
        sResponseMsg->set_fileexists(true);
        //Sets the size of the file
        sResponseMsg->set_filesize(fileStat.st_size);
        //Sets the checksum of the file
        sResponseMsg->set_filechecksum(dfs_file_checksum(filePath, &crc_table));
        //Sets when file was last modified
        auto mtime = fileStat.st_mtim;
        sResponseMsg->mutable_mtime()->set_seconds(mtime.tv_sec);
        //Sets when file was created
        auto ctime = fileStat.st_ctim;
        sResponseMsg->mutable_ctime()->set_seconds(ctime.tv_sec);

        dfs_log(LL_SYSINFO) << "ServerSide | Completed Client Request to send status of file: " << FileName;

        return Status::OK;
    }
    
    Status fileGetLocker(::grpc::ServerContext* context, const ::dfs_service::GetLockRequest* request, ::google::protobuf::Empty* response){
        std::string ClientID = request->clientid();
        std::string fileName = request->filename();

        dfs_log(LL_SYSINFO) << "-----------------------------------------------------------------";

        bool GotLocker = fileMutex_Request(fileName, ClientID);
        if(!GotLocker){
            return Status(StatusCode::RESOURCE_EXHAUSTED, "ServerSide | File Mutex is unavailable at this time");
        }



        return Status(StatusCode::OK, "ServerSide | Lock Operation accepted");
    }

    Status CallbackList(ServerContext* context, const ::dfs_service::CBLRequest* request, ::dfs_service::CBLResponse* response) override {
        //If Query is no longer needed

        dfs_log(LL_SYSINFO) << "-----------------------------------------------------------------";
        dfs_log(LL_SYSINFO) << "ServerSide | Accepting Client Request to send call back list of files in directory";
        dfs_log(LL_SYSINFO) << "Client has not cancelled request";

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
                dfs_service::CBLElementResponse* FileInfo = response->add_fileinfo();
                
                //Set file name
                FileInfo->set_filename(FileName);
                //Sets the size of the file
                FileInfo->set_filesize(FileOrDirectory.st_size);
                //Sets the checksum of the file
                FileInfo->set_filechecksum(dfs_file_checksum(CurrentPathNFile, &crc_table));
                //Sets when file was last modified
                auto mtime = FileOrDirectory.st_mtim;
                FileInfo->mutable_mtime()->set_seconds(mtime.tv_sec);
                //Sets when file was created
                auto ctime = FileOrDirectory.st_ctim;
                FileInfo->mutable_ctime()->set_seconds(ctime.tv_sec);

                dfs_log(LL_SYSINFO) << "ServerSide | Found File: " << FileName << " and timestamp: " << FileInfo->mutable_mtime()->seconds(); 
            }
        }
        //Freeing memory 
        closedir(dr);//Hmm


        dfs_log(LL_SYSINFO) << "ServerSide | Completed Client Request to send a list of files in directory"; 
        
        return Status::OK;
    }

    Status fileDeleter(ServerContext* context, const dfs_service::DeleteRequest* dRequestMsg, ::google::protobuf::Empty* dResponseMsg) override{        
        //Creating filePath string
        std::string FileName = dRequestMsg->filename();
        const std::string& filePath = WrapPath(FileName);
        std::string clientID = dRequestMsg->clientid();

        dfs_log(LL_SYSINFO) << "ServerSide | Attempting to delete file: " << clientID; 

        //Double checking the lock is correct
        if(!fileMutex_IsClientOwnerCheck(FileName, clientID)){
            dfs_log(LL_SYSINFO) << "ServerSide | Client ID [" << clientID <<  "] Request does not have lock to uodate stored file:" << FileName;
            dfs_log(LL_SYSINFO) << "ServerSide | Current Owner of file [" << FileName << "] is Client ID: [" << fileMutex_getOwner(FileName).c_str();
            return Status(StatusCode::CANCELLED, "ServerSide | Client should've had lock for file to peform this action");
        }
       
        dfs_log(LL_SYSINFO) << "ServerSide | Accepting Client Request to delete file: " << FileName; 

        //Mostly to keep track of whats going on
        struct stat fileStat;

        if(stat(filePath.c_str(), &fileStat) != 0){
            fileMutex_Delete(FileName, clientID);
            dfs_log(LL_ERROR) << "ServerSide | The requested file does not exist in the system: " << FileName;
            return Status(StatusCode::NOT_FOUND, "File was not found on server");
        }

        //If Query is no longer needed
        if(context->IsCancelled()){
            fileMutex_Release_Or_Delete(FileName, clientID, true);
            dfs_log(LL_ERROR) << "Deadline exceeded or Client cancelled request, prematurely ending request";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded ir Client cancelled, prematurely ending request");
        }

        //Trying to delete the file
        int TryDelFile = remove(filePath.c_str());
        if(TryDelFile != 0){
            dfs_log(LL_ERROR) << "Server unable to delete file: " << FileName;
            return Status(StatusCode::CANCELLED, "Server was unable to delete file on system");
        }

        dfs_log(LL_SYSINFO) << "ServerSide | Completed Client Request to delete file: " << FileName; 

        //Deleting lock
        dfs_log(LL_SYSINFO) << "ServerSide | Attempting to delete lock for file: " << FileName; 
        if(!fileMutex_Delete(FileName, clientID)){
            return Status(StatusCode::INTERNAL, "Error deleting lock");
        }

        return Status(StatusCode::OK, "File Deleted");
    }

    //Additional Proto functions:

    Status fileCheckSum(ServerContext* context, const dfs_service::CheckSumRequest* request, dfs_service::CheckSumResponse* response) override {
        std::string FileName = request->filename();
        dfs_log(LL_SYSINFO) << "ServerSide | Comparing client and server checksum for " << FileName;
        std::string filePath = WrapPath(FileName);
        uint32_t ClientCheckSum = request->checkvalue();
        std::string clientID = request->clientid();

        //Mostly to keep track of whats going on
        struct stat fileStat;
        if(stat(filePath.c_str(), &fileStat) != 0){
            dfs_log(LL_ERROR) << "ServerSide | Checksum for file, " << FileName <<"does not exist";
            return Status(StatusCode::NOT_FOUND, "Requested Status not found on server"); //TO DO needs to be not found
        }
        uint32_t ServerCheckSum = dfs_file_checksum(filePath, &crc_table);
        dfs_log(LL_SYSINFO) << "ServerSide | Setting Variables to compare checksum";
        response->set_filename(FileName);
        response->set_checkvalue(ServerCheckSum);
        dfs_log(LL_SYSINFO) << "ServerSide | Server File Checksum: " << ServerCheckSum;
        dfs_log(LL_SYSINFO) << "ServerSide | Client File Checksum: " << ClientCheckSum;

        if(ClientCheckSum == ServerCheckSum){
            dfs_log(LL_SYSINFO) << "ServerSide | CheckSum is the same for file [" << FileName << "]  on client and server";
            if(fileMutex_IsClientOwnerCheck(FileName, clientID)){
                fileMutex_Release(FileName, clientID);
            }
            return Status(StatusCode::ALREADY_EXISTS, "File Already exists");
        }
        else if (ClientCheckSum != ServerCheckSum) {
            dfs_log(LL_SYSINFO) << "ServerSide | CheckSum is the different for file [" << FileName << "]  on client and server";
            return Status::OK;
        }

        dfs_log(LL_ERROR) << "ServerSide | Checksums could not be compared for file:  " << FileName;
        
        return Status(StatusCode::CANCELLED, "ServerSide | Unsure why CheckSum failed");
    }

    Status fileSameTimestamp(::grpc::ServerContext* context, const ::dfs_service::TimeStampRequest* request, ::dfs_service::TimeStampResponse* response){
        std::string FileName = request->filename();
        dfs_log(LL_SYSINFO) << "ServerSide | Comparing client and server timestamp for " << FileName;

        std::string filePath = WrapPath(FileName);
        time_t fileClient_mTime = request->mtime().seconds();

        dfs_log(LL_SYSINFO) << "ServerSide | Last modified on clients system: " << fileClient_mTime;



        //Mostly to keep track of whats going on
        struct stat fileStat;
        if(stat(filePath.c_str(), &fileStat) != 0){
            dfs_log(LL_ERROR) << "ServerSide | Timestamp for file, " << FileName <<"does not exist";
            return Status(StatusCode::NOT_FOUND, "Requested Status not found on server"); //TO DO needs to be not found
        }

        //Grabbing when file was last modified
        time_t fileServer_mTime = (time_t) fileStat.st_mtim.tv_sec;
        dfs_log(LL_SYSINFO) << "ServerSide | Last modified on server system: " << fileServer_mTime;

        if(fileServer_mTime == fileClient_mTime){
            response->set_sametimestamp(true);
            dfs_log(LL_SYSINFO) << "ServerSide | Time stamps are the same for server and client file:  " << FileName;
            return Status(StatusCode::OK, "Timestamp exists but same");
        }
        else if (fileServer_mTime != fileClient_mTime){
            response->set_sametimestamp(false);
            dfs_log(LL_SYSINFO) << "ServerSide | Time stamps are the different for server and client file:  " << FileName;
            return Status(StatusCode::OK, "Different timestamps between client and server");

        }

        dfs_log(LL_ERROR) << "ServerSide | Timestamps could not be compared for file:  " << FileName;
        
        return Status(StatusCode::CANCELLED, "ServerSide | Unsure why retrieving timestamp failed");
    }



};


DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

