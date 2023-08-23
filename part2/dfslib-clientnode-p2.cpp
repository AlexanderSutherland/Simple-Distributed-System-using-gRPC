#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

#define FILECHUNKBUFSIZE 4096

extern dfs_log_level_e DFS_LOG_LEVEL;

using FileRequestType = dfs_service::CBLRequest;
using FileListResponseType = dfs_service::CBLResponse;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {


    //Logging for potential debugging
    dfs_log(LL_SYSINFO) << "ClientSide | Requesting write access for " << filename;

    //Adding deadline exceeded timer
    ClientContext clientContext;
    clientContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout));  

    //Message structs
    dfs_service::GetLockRequest glRequestMsg;
    ::google::protobuf::Empty glResponseMsg;
    
    //File in request data
    glRequestMsg.set_filename(filename);
    glRequestMsg.set_clientid(ClientId());

    //Create gRPC proto
    Status msgStatus = service_stub->fileGetLocker(&clientContext, glRequestMsg, &glResponseMsg);
    if(!msgStatus.ok()){
        dfs_log(LL_ERROR) << "Could not get lock for file, " << filename << ". Error Message: " << msgStatus.error_message();
        if(msgStatus.error_code() == StatusCode::RESOURCE_EXHAUSTED){
            return StatusCode::RESOURCE_EXHAUSTED;
        }
        else if(msgStatus.error_code() == StatusCode::DEADLINE_EXCEEDED){
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else{
            return StatusCode::CANCELLED;
        }
    }

    dfs_log(LL_SYSINFO) << "Write lock request for file, " << filename << " was successful";

    return StatusCode::OK; 


}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //////////////////////////////////////////
    //Setting Deadline and initial Variables//
    //////////////////////////////////////////

    //Logging for potential debugging
    dfs_log(LL_SYSINFO) << "ClientSide | Requesting to store file: " << filename;

    //Adding deadline exceeded timer
    ClientContext clientContext;
    clientContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout));  

    //Creating filePath string
    const std::string& filePath = WrapPath(filename);

    //Grabbing client file info and returning Not found is stats aren't found
    struct stat fileStat;
    if(stat(filePath.c_str(), &fileStat) != 0){
        dfs_log(LL_SYSINFO) << "ClientSide | Given file not found: " << filename;
        return StatusCode::NOT_FOUND;
    }
    off_t fileSize = fileStat.st_size;
    time_t mtime = fileStat.st_mtim.tv_sec;

    //If file exist but is empty
    if(fileSize < 1){
        dfs_log(LL_ERROR) << "ClientSide | File is a size of " << fileSize;
        return StatusCode::NOT_FOUND;
    }

    //////////////////////////////////////////////////////////////
    //Request Server for writer lock if check sums are different//
    //////////////////////////////////////////////////////////////
    StatusCode msgGotLockStatus = RequestWriteAccess(filename);
    if(msgGotLockStatus != StatusCode::OK){
        dfs_log(LL_ERROR) << "ClientSide | Could not get Writer Lock for file [" << filename << "] Upload Request StatusCode: " << msgGotLockStatus;
        return msgGotLockStatus;
    }

    //////////////////////////////////////////////////////////////////////////////
    //Send file over grpc to Serve Section if getting writer lock was successful//
    //////////////////////////////////////////////////////////////////////////////

    //fileChunk which will be used to fill data of the file we want to store
    std::vector<char> fileChunk(FILECHUNKBUFSIZE, 0);

    //Opening file in read mode
    std::ifstream file;
    file.open(filePath, std::ios::in);

    //Creating variables for fileUploadRequest function and populating them 
    dfs_service::UploadResponse FileUploadResponse;
    std::unique_ptr<ClientWriter<dfs_service::UploadRequest>> cwriter (service_stub->fileUploadRequest(&clientContext, &FileUploadResponse));
    dfs_service::UploadRequest FileUploadRequest;
    FileUploadRequest.set_filename(filename);
    FileUploadRequest.set_filesize(fileSize);
    FileUploadRequest.set_clientid(ClientId());
    FileUploadRequest.set_cfilechecksum(dfs_file_checksum(filePath, &crc_table));
    FileUploadRequest.mutable_cfilemtime()->set_seconds(mtime);

    //Logging
    dfs_log(LL_SYSINFO) << "ClientSide | Beginning upload of file: " << filename;


    //Reading bytes of the file and sending it through a stream msg
    std::size_t bytesRead = 0;
    while(!file.eof()){
        if(bytesRead >= fileSize){
            dfs_log(LL_SYSINFO) << "ClientSide | Bytes should be full sent to Server: " << bytesRead << "/" << fileSize;
            break;
        }
        if(bytesRead + fileChunk.size() > fileSize){
            file.read(fileChunk.data(), fileSize-bytesRead);
            FileUploadRequest.set_filechunksize(file.gcount());
            FileUploadRequest.set_filechunk(fileChunk.data(), file.gcount());
            bytesRead += file.gcount();
        }
        else{
            file.read(fileChunk.data(), fileChunk.size());
            FileUploadRequest.set_filechunksize(file.gcount());
            FileUploadRequest.set_filechunk(fileChunk.data(), file.gcount());
            bytesRead += file.gcount();

        }
        //bytes are not being read cancel request
        if(bytesRead == 0){
            dfs_log(LL_ERROR) << "ClientSide | Bytes could not be read. Canceling Request";
            return StatusCode::CANCELLED;
        }
        dfs_log(LL_SYSINFO) << "ClientSide | Bytes uploaded to Server: " << bytesRead << "/" << fileSize;
        cwriter->Write(FileUploadRequest);
    }
    cwriter->WritesDone();
    Status fileUploadStatus = cwriter->Finish();
    dfs_log(LL_SYSINFO) << "ClientSide | File upload stream completed for " << filename;
    
    //Close file no longer needed
    file.close();

    dfs_log(LL_SYSINFO) << "Clientside | Status Code: " << fileUploadStatus.error_code();
    
    //Returning the error code if not Ok
    if(!fileUploadStatus.ok()){
        if(fileUploadStatus.error_code() == StatusCode::CANCELLED){
            dfs_log(LL_ERROR) << "ClientSide | File Upload Request StatusCode: CANCELLED";
            return StatusCode::CANCELLED;
        }
        else if(fileUploadStatus.error_code() == StatusCode::DEADLINE_EXCEEDED){
            dfs_log(LL_ERROR) << "ClientSide | File Upload Request StatusCode: DEADLINE_EXCEEDED";
            return StatusCode::DEADLINE_EXCEEDED;
        }

        else if(fileUploadStatus.error_code() == StatusCode::RESOURCE_EXHAUSTED){
            dfs_log(LL_ERROR) << "ClientSide | File Upload Request StatusCode: RESOURCE EXHAUSTED";
            return StatusCode::RESOURCE_EXHAUSTED;
        }
        else if(fileUploadStatus.error_code() == StatusCode::ALREADY_EXISTS){
            dfs_log(LL_ERROR) << "ClientSide | File Upload Request StatusCode: ALREADY_EXISTS";
            return StatusCode::ALREADY_EXISTS;
        }
        else{
            dfs_log(LL_ERROR) << "ClientSide | File Upload Request StatusCode: UNKNOWN -> CANCELLED";
            dfs_log(LL_ERROR) << "ClientSide | Error Message: " << fileUploadStatus.error_message();
            return StatusCode::CANCELLED;
        }
    }

    dfs_log(LL_SYSINFO) << "File Upload Request StatusCode: OK";
    return StatusCode::OK;

}



grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    ///////////////////////////////////////////////////////////
    //Setting Deadline, logging and setting initial variables//
    ///////////////////////////////////////////////////////////

    //Adding info of request
    dfs_log(LL_SYSINFO) << "ClientSide Fetch | Requesting to fetch file: " << filename;

    //Adding deadline exceeded timer
    ClientContext clientContext;
    clientContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout));  
    
    //Creating filePath string
    const std::string& filePath = WrapPath(filename);
    dfs_log(LL_SYSINFO) << "ClientSide Fetch | Creating Path: " << filePath;

    //Mostly to keep track of whats going on
    struct stat fileStat;
    bool FileInClient = true;
    if(stat(filePath.c_str(), &fileStat) != 0){
        FileInClient = false;
        dfs_log(LL_SYSINFO) << "ClientSide Fetch | Given file not found in system will be creating the file: " << filename;
    } 
    else{
        dfs_log(LL_SYSINFO) << "ClientSide Fetch | Given file found in system will be over written file: " << filename;
        
        //TO DO do a checksum to see if the message needs to be sent otherwise don't do it
    }

    ////////////////////////////////////////////////////////////////////
    //Create fetch request                                            //
    ////////////////////////////////////////////////////////////////////

    //Creating variables for fileUploadRequest function and populating them 
    dfs_service::FetchRequest fRequestMsg;
    fRequestMsg.set_filename(filename);
    if(FileInClient){
        fRequestMsg.set_clienthasfile(true);
        fRequestMsg.set_cfilechecksum(dfs_file_checksum(filePath, &crc_table));
        fRequestMsg.mutable_cfilemtime()->set_seconds(fileStat.st_mtim.tv_sec);
    }
    else{
        fRequestMsg.set_clienthasfile(false);
    }

    dfs_service::FetchResponse fResponseMsg;
    std::unique_ptr<ClientReader<dfs_service::FetchResponse>> creader (service_stub->fileFetcher(&clientContext, fRequestMsg));

    //Create file to be written into with the creader info

    //First read incase the file is zero don't erase local copy
    creader->Read(&fResponseMsg);
    size_t bytesRead = 0;
    size_t fileSize = fResponseMsg.filesize();
    
    /*
    if(fileSize <= 0){
        dfs_log(LL_ERROR) << "ClientSide Fetch | Server file is zero or empty: " << filename;
        return StatusCode::CANCELLED;
    }
    */
    //Checks if we should copy the file over or not
    if(fResponseMsg.copyfile()){        
        dfs_log(LL_SYSINFO) << "ClientSide Fetch | Beginning to grab the data of file: " << filename;
        //Opening the file to write to
        std::ofstream file;
        file.open(filePath, std::ios::out | std::ios::trunc);

        const std::string chunkContents = fResponseMsg.content();
        file << chunkContents;
        bytesRead += fResponseMsg.content().length();
        dfs_log(LL_SYSINFO) << "ClientSide | Bytes Download from Server: " << bytesRead << "/" << fileSize;

        while(creader->Read(&fResponseMsg)){
            if(fileSize  <= 0){
                fileSize = fResponseMsg.filesize();
            }
            const std::string chunkContents = fResponseMsg.content();
            file << chunkContents;
            bytesRead += fResponseMsg.content().length();
            dfs_log(LL_SYSINFO) << "ClientSide | Bytes Download from Server: " << bytesRead << "/" << fileSize;
        }
        file.close();
    }
    Status StatusMsg = creader->Finish();
    //Comment

    //Log the StatusCode is it's an error
    if(!StatusMsg.ok()){
        if(StatusMsg.error_code() == StatusCode::CANCELLED){
            dfs_log(LL_ERROR) << "ERROR: StatusCode::CANCELLED for requesting to fetch file: " << filename;
            return StatusCode::CANCELLED;
        }
        else if(StatusMsg.error_code() == StatusCode::DEADLINE_EXCEEDED){
            dfs_log(LL_ERROR) << "ERROR: StatusCode::DEADLINE_EXCEEDED for requesting to delete file: " << filename;
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else if (StatusMsg.error_code() == StatusCode::NOT_FOUND){
            dfs_log(LL_ERROR) << "ERROR: StatusCode::NOT_FOUND for requesting to fetch file: " << filename;
            return StatusCode::NOT_FOUND;
        }
        else if (StatusMsg.error_code() == StatusCode::ALREADY_EXISTS){
            dfs_log(LL_ERROR) << "ERROR: StatusCode::NOT_FOUND for requesting to fetch file: " << filename;
            return StatusCode::ALREADY_EXISTS;
        }
        else{
            dfs_log(LL_ERROR) << "ERROR: StatusCode::CANCELLED for requesting to fetch file: " << filename;
            return StatusCode::CANCELLED;
        }
    }
    
    //Log the server removed the file
    dfs_log(LL_SYSINFO) << "ClientSide | The following file was fetched from the server: " << filename;
    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //////////////////////////////////////////////////////////////
    //Deadline set and some logging                             //
    //////////////////////////////////////////////////////////////

    //Adding info of request
    dfs_log(LL_SYSINFO) << "ClientSide | Requesting to delete file: " << filename;

    //Adding deadline exceeded timer
    ClientContext clientContext;
    clientContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout));  
    
    //////////////////////////////////////////////////////////////
    //Request Server for writer lock if check sums are different//
    //////////////////////////////////////////////////////////////
    StatusCode msgGotLockStatus = RequestWriteAccess(filename);
    if(msgGotLockStatus != StatusCode::OK){
        return msgGotLockStatus;
    }

    //////////////////////////////////////////////////////////////
    //Request Server to delete file after writer lock is given  //
    //////////////////////////////////////////////////////////////

    //Creating Request and Response Msg
    dfs_service::DeleteRequest dRequestMsg;
    dRequestMsg.set_filename(filename);
    dRequestMsg.set_clientid(ClientId());
    ::google::protobuf::Empty dResponseMsg;

    //Create Request to delete file
    Status msgStatus = service_stub->fileDeleter(&clientContext, dRequestMsg, &dResponseMsg);

    //Log the StatusCode is it's an error
    if(!msgStatus.ok()){
        if(msgStatus.error_code() == StatusCode::CANCELLED){
            dfs_log(LL_ERROR) << "ERROR: StatusCode::CANCELLED for requesting to delete file: " << filename;
            return StatusCode::CANCELLED;
        }
        else if(msgStatus.error_code() == StatusCode::DEADLINE_EXCEEDED){
            dfs_log(LL_ERROR) << "ERROR: StatusCode::DEADLINE_EXCEEDED for requesting to delete file: " << filename;
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else if (msgStatus.error_code() == StatusCode::NOT_FOUND){
            dfs_log(LL_ERROR) << "ERROR: StatusCode::NOT_FOUND for requesting to delete file: " << filename;
            return StatusCode::NOT_FOUND;
        }
        else{
            dfs_log(LL_ERROR) << "ERROR: StatusCode::CANCELLED for requesting to delete file: " << filename;
            return StatusCode::CANCELLED;
        }
    }

    //Log the server removed the file
    dfs_log(LL_SYSINFO) << "ClientSide | The following file was deleted from the server: " << filename;
    return StatusCode::OK;

}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //Logging begining to request
    dfs_log(LL_SYSINFO) << "ClientSide | Requesting to a list of files on server";

    //Adding deadline exceeded timer
    ClientContext clientContext;
    clientContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout));  
    
    //Create Msg Variables (Structures)
    dfs_service::ListResponse filesList;
    const ::google::protobuf::Empty request;

    //Create Request
    Status msgStatus = service_stub->fileLister(&clientContext, request, &filesList);

    //Log the StatusCode is it's an error
    if(!msgStatus.ok()){
        if(msgStatus.error_code() == StatusCode::CANCELLED){
            dfs_log(LL_ERROR) << "ClientSide | ERROR: StatusCode::CANCELLED for requesting a list and timestamp";
            return StatusCode::CANCELLED;
        }
        else if(msgStatus.error_code() == StatusCode::DEADLINE_EXCEEDED){
            dfs_log(LL_ERROR) << "ClientSide | ERROR: StatusCode::DEADLINE_EXCEEDED for requesting a list and timestamp";
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else if (msgStatus.error_code() == StatusCode::NOT_FOUND){
            dfs_log(LL_ERROR) << "ClientSide | ERROR: StatusCode::NOT_FOUND for requesting a list and timestamp";
            return StatusCode::NOT_FOUND;
        }
        else{
            dfs_log(LL_ERROR) << "ERROR: Unkown setting to StatusCode::CANCELLED for requesting a list and timestamp";
            return StatusCode::CANCELLED;
        }
    }
    
    //Reading the repeated messages
    dfs_log(LL_SYSINFO) << "ClientSide | Client is reading message of list of files and time stamps now";
    for(dfs_service::ListElementResponse& Element : *filesList.mutable_file()){
        std::string fileName = Element.filename();
        int mTime = Element.mtime().seconds();
        dfs_log(LL_SYSINFO) << "ClientSide | Client has recieved file: " << fileName << " and timestamp:" << mTime;
        file_map->insert({fileName,mTime});
    }

    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //Logging begining to request
    dfs_log(LL_SYSINFO) << "ClientSide | Requesting to a status of file:" << filename;

    //Adding deadline exceeded timer
    ClientContext clientContext;
    clientContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout));  

    //Create Msg Variables (Structures)
    dfs_service::StatusRequest sRequestMsg;
    dfs_service::StatusResponse sResponseMsg;

    //Setting request info
    sRequestMsg.set_filename(filename);

    //Create Request
    Status msgStatus = service_stub->fileStatuser(&clientContext, sRequestMsg, &sResponseMsg);

    //Log the StatusCode is it's an error
    if(!msgStatus.ok()){
        if(msgStatus.error_code() == StatusCode::CANCELLED){
            dfs_log(LL_ERROR) << "ClientSide | StatusCode::CANCELLED for requesting status of file: " << filename;
            return StatusCode::CANCELLED;
        }
        else if(msgStatus.error_code() == StatusCode::DEADLINE_EXCEEDED){
            dfs_log(LL_ERROR) << "ClientSide | StatusCode::DEADLINE_EXCEEDED for requesting status of file: " << filename;
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else if (msgStatus.error_code() == StatusCode::NOT_FOUND){
            dfs_log(LL_ERROR) << "ClientSide | StatusCode::NOT_FOUND for requesting status of file: " << filename;
            return StatusCode::NOT_FOUND;
        }
        else{
            dfs_log(LL_ERROR) << "Unkown setting to StatusCode::CANCELLED for requesting status of file: " << filename;
            return StatusCode::CANCELLED;
        }
    }

    //Logging begining to request
    dfs_log(LL_SYSINFO) << "ClientSide | File Name: " << filename << " | Stats [ File Size: " << sResponseMsg.filesize() << "| Last Modified: " << sResponseMsg.mtime().seconds() << " ]";

    return StatusCode::OK;
}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //Created critical sections which only broadcast once one has completed
    dfs_log(LL_SYSINFO) << "ClientSide | Client is performing a Inotify Callback";
    dfs_log(LL_SYSINFO) << "ClientSide | Inotify waiting to acquire the lock";
    //Acquire the lock
    std::unique_lock<std::mutex> AT_Lock (AT_Mtx);
    //Wait on conditonal
    while(!AT_Lock_Available){AT_CV.wait(AT_Lock);}
    //Update conditonal to unavailable
    dfs_log(LL_SYSINFO) << "ClientSide | Inotify has acquired the lock";
    AT_Lock_Available = false;
    AT_Lock.unlock();

    callback();

    //Update conditional to available and broadcast it's available
    AT_Lock.lock();
    AT_Lock_Available = true;
    AT_Lock.unlock();
    dfs_log(LL_SYSINFO) << "ClientSide | Inotify is done using the lock, broadcasting lock availability";
    AT_CV.notify_all();
    

}


void DFSClientNodeP2::HandleCallbackList() {


    bool ok = false;
    void* tag;

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {

            dfs_log(LL_SYSINFO) << "ClientSide | Client is performing a Callbacklist";
            dfs_log(LL_SYSINFO) << "ClientSide | Callbacklist waiting to acquire the lock";
            //Acquire the lock
            std::unique_lock<std::mutex> AT_Lock (AT_Mtx);
            //Wait on conditonal
            while(!AT_Lock_Available){AT_CV.wait(AT_Lock);}
            dfs_log(LL_SYSINFO) << "ClientSide | Callbacklist has acquired the lock";
            //Update conditonal
            AT_Lock_Available = false;
            AT_Lock.unlock();

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Completion queue callback triggered";

            // Verify that the request was completed successfully
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";



                dfs_log(LL_SYSINFO) << "ClientSide | Going the call data from callbacklist";
                for(const dfs_service::CBLElementResponse& Element : call_data->reply.fileinfo()){
                    std::string fileName = Element.filename();
                    std::string filePath = WrapPath(fileName);
                    dfs_log(LL_SYSINFO) << "ClientSide | Comparing file [" << fileName << "] with server's";
                    //Check if we have the file
                    struct stat fileStat;
                    if(stat(filePath.c_str(), &fileStat) != 0){
                        dfs_log(LL_SYSINFO) << "ClientSide | Given file not found in system will be calling fetch method for file: " << fileName;
                        Fetch(fileName);
                    }

                    //Check if the checksums are the same
                    uint32_t ClientFileCheckSum = dfs_file_checksum(filePath, &crc_table);
                    uint32_t ServerFileCheckSum = Element.filechecksum();
                    if(ClientFileCheckSum != ServerFileCheckSum){
                        //If different checksum then compare the modified times
                        time_t ClientFile_mtime = fileStat.st_mtim.tv_sec;
                        time_t ServerFile_mtime = Element.mtime().seconds();
                        //If Client file is newer store it
                        if(ClientFile_mtime > ServerFile_mtime){
                            dfs_log(LL_SYSINFO) << "ClientSide | File was last modified at main server calling Fetch method";
                            Store(fileName);
                        }
                        //If Server file is newer fetch it
                        else if(ClientFile_mtime < ServerFile_mtime){
                            dfs_log(LL_SYSINFO) << "ClientSide | File was last modified at client server calling Store method";
                            Fetch(fileName);
                        }
                        //We should not be here
                        else{
                            dfs_log(LL_ERROR) << "ClientSide | Checksums are different but client and server times are the same????";
                        }
                    }
                    //If the checksums are the same
                    else{
                        dfs_log(LL_SYSINFO) << "ClientSide | File checksum is the same on client and server. No action taken";
                    }
                }

                //Unlock the asynchronous lock and let other threads now it's available
                AT_Lock.lock();
                AT_Lock_Available = true;
                AT_Lock.unlock();
                dfs_log(LL_SYSINFO) << "ClientSide | Callbacklist is done using the lock, broadcasting lock availability";
                AT_CV.notify_all();

            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;


        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}


void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}


