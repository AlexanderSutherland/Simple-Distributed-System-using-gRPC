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

#include "dfs-utils.h"
#include "dfslibx-clientnode-p2.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

extern dfs_log_level_e DFS_LOG_LEVEL;

DFSClientNode::DFSClientNode() : mount_path("mnt/client/"), unmounting(false), crc_table(CRC::CRC_32()) {
    char host[HOST_NAME_MAX];
    std::ostringstream stream_id;
    gethostname(host, HOST_NAME_MAX);
    auto t_id = std::this_thread::get_id();
    stream_id << "T" << t_id;
    client_id = std::string(host + stream_id.str());
}

const std::string DFSClientNode::ClientId() {
    return this->client_id;
}

DFSClientNode::~DFSClientNode() noexcept {}

void DFSClientNode::Unmount() {
    this->unmounting = true;
}

bool DFSClientNode::Unmounting() {
    return this->unmounting;
}

void DFSClientNode::CreateStub(std::shared_ptr <Channel> channel) {
    this->service_stub = dfs_service::DFSService::NewStub(channel);
}

void DFSClientNode::SetMountPath(const std::string &path) {
    this->mount_path = path;
}

const std::string DFSClientNode::MountPath() {
    return this->mount_path;
};

void DFSClientNode::SetDeadlineTimeout(int deadline) {
    this->deadline_timeout = deadline;
}

void DFSClientNode::SetClientId(const std::string &id) {
    this->client_id = id;
}

std::string DFSClientNode::WrapPath(const std::string &filepath) {
    return this->mount_path + filepath;
}

