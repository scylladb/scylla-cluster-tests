#include <boost/program_options.hpp>
#include <cassandra.h>
#include <iostream>
#include <string>
#include <chrono>
#include <thread>

namespace po = boost::program_options;

int main(int argc, char **argv) {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help",    "produce help message")
        ("servers",  po::value<std::string>(), "set servers ip separated by comas")
        ("duration", po::value<int>(), "duration in s")
        ("queries",  po::value<int>(), "number of queries")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 1;
    }

    std::string servers;
    if (vm.count("servers")) {
        servers = vm["servers"].as<std::string>();
    } else {
        std::cout << desc << std::endl;
        return 1;
    }

    int duration;
    if (vm.count("duration")) {
        duration = vm["duration"].as<int>();
    } else {
        std::cout << desc << std::endl;
        return 1;
    }

    int queries;
    if (vm.count("queries")) {
        queries = vm["queries"].as<int>();
    } else {
        std::cout << desc << std::endl;
        return 1;
    }

    CassFuture* future = nullptr;
    auto cluster = cass_cluster_new();
    cass_cluster_set_protocol_version(cluster, 3);
    auto session = cass_session_new();

    /* Add contact points */
    cass_cluster_set_contact_points(cluster, servers.c_str());
    cass_cluster_set_pending_requests_high_water_mark(cluster, 1000000);
    cass_cluster_set_write_bytes_high_water_mark(cluster, 100000000);

    /* Provide the cluster object as configuration to connect the session */
    future = cass_session_connect(session, cluster);

    if (cass_future_error_code(future) != CASS_OK) {
        std::cout << "Unable to connect" << std::endl;
        return 2;
    }
    cass_future_free(future);

    auto statement = cass_statement_new("CREATE KEYSPACE ks WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };", 0);
    future = cass_session_execute(session, statement);
    if (cass_future_error_code(future) != CASS_OK) {
        std::cout << "Unable to create keyspace" << std::endl;
        return 3;
    }
    cass_statement_free(statement);
    cass_future_free(future);

    statement = cass_statement_new("CREATE TABLE ks.test (id int PRIMARY KEY, payload blob)", 0);
    future = cass_session_execute(session, statement);
    if (cass_future_error_code(future) != CASS_OK) {
        std::cout << "Unable to create table" << std::endl;
        return 3;
    }
    cass_statement_free(statement);
    cass_future_free(future);


    auto blob = std::unique_ptr<char>(new char[4096]());
    auto blob_string = std::string(blob.get());

    CassFuture* futures[queries];

    for (int i = 0; i < queries; i++) {
        auto query = "insert into ks.test  (id, payload) values (" + std::to_string(i) + ", textAsBlob('"+ blob_string +"'));";
        statement = cass_statement_new(query.c_str(), 0);
        futures[i] = cass_session_execute(session, statement);
        cass_statement_free(statement);
    }

    std::cout << "Queries sent" << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(duration));

    for (int i = 0; i < queries; i++) {
        cass_future_free(futures[i]);
    }

    cass_cluster_free(cluster);
    cass_session_free(session);

    return 0;
}
