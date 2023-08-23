#include <regex>
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

#include "dfslib-shared-p1.h"
#include "dfslib-clientnode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

#define FILECHUNKBUFSIZE 4096

//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the 'dfs_service' namespace.
//
// For example, if you have a method named MyCoolMethod, add
// the following:
//
//      using dfs_service::MyCoolMethod
//


DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

StatusCode DFSClientNodeP1::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
    // StatusCode::CANCELLED otherwise
    //

    //Logging for potential debugging
    dfs_log(LL_SYSINFO) << "ClientSide | Requesting to store file: " << filename;

    //Adding deadline exceeded timer
    ClientContext clientContext;
    clientContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout));  

    //Creating filePath string
    const std::string& filePath = WrapPath(filename);
    dfs_log(LL_SYSINFO) << "ClientSide | Creating Path: " << filePath;

    //Grabbing file info and returning Not found is stats aren't found
    struct stat fileStat;
    if(stat(filePath.c_str(), &fileStat) != 0){
        dfs_log(LL_SYSINFO) << "ClientSide | Given file not found: " << filename;
        return StatusCode::NOT_FOUND;
    }
    off_t fileSize = fileStat.st_size;

    //If file exist but is empty
    if(fileSize < 1){
        dfs_log(LL_ERROR) << "ClientSide | File is a size of " << fileSize;
        return StatusCode::NOT_FOUND;
    }

    //Reading file we want to store
    //char fileChunk[FILECHUNKBUFSIZE];
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
        else{
            dfs_log(LL_ERROR) << "ClientSide | File Upload Request StatusCode: UNKNOWN -> CANCELLED";
            dfs_log(LL_ERROR) << "ClientSide | Error Message: " << fileUploadStatus.error_message();
            return StatusCode::CANCELLED;
        }
    }

    dfs_log(LL_SYSINFO) << "File Upload Request StatusCode: OK";
    return StatusCode::OK;
    
}


StatusCode DFSClientNodeP1::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    //Adding info of request
    dfs_log(LL_SYSINFO) << "ClientSide | Requesting to fetch file: " << filename;

    //Adding deadline exceeded timer
    ClientContext clientContext;
    clientContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout));  
    
    //Creating filePath string
    const std::string& filePath = WrapPath(filename);
    dfs_log(LL_SYSINFO) << "ClientSide | Creating Path: " << filePath;

    //Creating variables for fileUploadRequest function and populating them 
    dfs_service::FetchRequest fRequestMsg;
    fRequestMsg.set_filename(filename);
    dfs_service::FetchResponse fResponseMsg;
    std::unique_ptr<ClientReader<dfs_service::FetchResponse>> creader (service_stub->fileFetcher(&clientContext, fRequestMsg));
    
    //Mostly to keep track of whats going on
    struct stat fileStat;
    if(stat(filePath.c_str(), &fileStat) != 0){
        dfs_log(LL_SYSINFO) << "ClientSide | Given file not found in system will be creating the file: " << filename;
    } 
    else{
        dfs_log(LL_SYSINFO) << "ClientSide | Given file found in system will be over writing file: " << filename;
    }

    //Opening the file to write to
    std::ofstream file;
    file.open(filePath, std::ios::out | std::ios::trunc);

    size_t fileSize = -1;
    size_t bytesRead = 0;

    //Create file to be written into with the creader info
    while(creader->Read(&fResponseMsg)){
        if(fileSize == -1){
            fileSize = fResponseMsg.filesize();
        }
        const std::string chunkContents = fResponseMsg.content();
        file << chunkContents;
        bytesRead += fResponseMsg.content().length();
        dfs_log(LL_SYSINFO) << "ClientSide | Bytes Download from Server: " << bytesRead << "/" << fileSize;
    }
    file.close();
    Status StatusMsg = creader->Finish();

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
        else{
            dfs_log(LL_ERROR) << "ERROR: StatusCode::CANCELLED for requesting to fetch file: " << filename;
            return StatusCode::CANCELLED;
        }
    }
    
    //Log the server removed the file
    dfs_log(LL_SYSINFO) << "ClientSide | The following file was fetched from the server: " << filename;
    return StatusCode::OK;
}

StatusCode DFSClientNodeP1::Delete(const std::string& filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //

    //Adding info of request
    dfs_log(LL_SYSINFO) << "ClientSide | Requesting to delete file: " << filename;

    //Adding deadline exceeded timer
    ClientContext clientContext;
    clientContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout));  
    
    //Creating Request and Response Msg
    dfs_service::DeleteRequest dRequestMsg;
    dRequestMsg.set_filename(filename);
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

StatusCode DFSClientNodeP1::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //

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

StatusCode DFSClientNodeP1::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

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

//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//


