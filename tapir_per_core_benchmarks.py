from collections import namedtuple
from pyrem.host import RemoteHost
from pyrem.task import Parallel
import argparse
import datetime
import json
import os
import os.path
import time

# A set of benchmark parameters.
Parameters = namedtuple('Parameters', [
    # Client and server parameters. ############################################
    # The directory in which TAPIR config files are stored.
    'config_file_directory',
    # The maximum number of allowable failures.
    'f',
    # The number of data shards. One server is run for every replica of every
    # shard. In total, there will be `(2f + 1) * num_shards` shards.
    'num_shards',
    # The text file that contains the keys that are preloaded into TAPIR.
    'key_file',
    # The number of keys to read from the key file.
    'num_keys',

    # Client parameters. #######################################################
    # The number of seconds that the clients run.
    'benchmark_duration_seconds',
    # The number of operations (i.e., reads and writes) within a transaction.
    'transaction_length',
    # The percentage of operations in a transaction that are writes.
    'write_percentage',
    # The zipfian coefficient used to select keys.
    'zipf_coefficient',
    # The number of client machines on which we run clients. Can be between 1
    # and 5 since we have 5 client machines.
    'num_client_machines',
    # The number of clients to run on every machine. There are
    # `num_client_machines * num_clients_per_machine` total clients.
    'num_clients_per_machine',
    # The directory into which benchmark results are written. If the output
    # directory is foo/, then output is written to a directory foo/1247014124
    # where 1247014124 is a unique id for the benchmark.
    'output_directory',
])

def run_benchmark(clients, servers, parameters):
    # Start the servers.
    server_tasks = []
    for (replica_index, server) in enumerate(servers[:2*parameters.f + 1]):
        for shard in range(parameters.num_shards):
            cmd = [
                "taskset", "-c", str(shard),
                "syslab/tapir/store/tapirstore/server",
                "-c", os.path.join(
                    parameters.config_file_directory,
                    'f{}.shard{}.config'.format(parameters.f, shard)),
                "-i", str(replica_index),
                "-n", str(shard),
                "-N", str(parameters.num_shards),
                "-f", parameters.key_file,
                "-k", str(parameters.num_keys),
                "-m", "txn-l",
            ]
            print('Running {} on {}.'.format(' '.join(cmd), server.hostname))
            server_tasks.append(server.run(cmd))
    parallel_server_tasks = Parallel(server_tasks)
    parallel_server_tasks.start()

    # Wait for the servers to start.
    print('Sleeping 5 seconds.')
    time.sleep(5)

    # Create an output directory for the clients. We assume that the machine on
    # which this script is running shares file system with the clients.
    output_directory = os.path.join(parameters.output_directory,
                                    str(int(time.time() * 1e6)))
    os.mkdir(output_directory)
    print('Writing benchmark results to {}.'.format(output_directory))

    # Inside this directory, we write the start time, the parameters of the
    # benchmark, and the end time of the benchmark.
    with open(os.path.join(output_directory, 'start_time.txt'), 'w') as f:
        f.write(str(datetime.datetime.now()))
        f.write('\n')

    with open(os.path.join(output_directory, 'parameters.json'), 'w') as f:
        f.write(json.dumps(parameters._asdict(), indent=4, sort_keys=True))
        f.write('\n')

    # Start the clients and wait for them to finish.
    client_tasks = []
    for client in clients[:parameters.num_client_machines]:
        for i in range(parameters.num_clients_per_machine):
            cmd = [
                "syslab/tapir/store/benchmark/benchClient",
                    "-c", os.path.join(
                        parameters.config_file_directory,
                        'f{}.shard'.format(parameters.f)),
                    "-f", parameters.key_file,
                    "-N", str(parameters.num_shards),
                    "-d", str(parameters.benchmark_duration_seconds),
                    "-l", str(parameters.transaction_length),
                    "-w", str(parameters.write_percentage),
                    "-k", str(parameters.num_keys),
                    "-r", "0",
                    "-m", "txn-l",
                    "-z", str(parameters.zipf_coefficient),
                "2>&1", "|",
                "tee", os.path.join(output_directory,
                                    '{}_{}.csv'.format(client.hostname, i))
            ]
            print('Running {} on {}.'.format(' '.join(cmd), client.hostname))
            client_tasks.append(client.run(cmd))
    parallel_client_tasks = Parallel(client_tasks)
    parallel_client_tasks.start()
    parallel_client_tasks.wait()

    # Write the end time to the output directory.
    with open(os.path.join(output_directory, 'end_time.txt'), 'w') as f:
        f.write(str(datetime.datetime.now()))
        f.write('\n')

    # Kill the servers.
    parallel_server_tasks.stop()


def main(args):
    # Sanity check command line arguments.
    assert os.path.exists(args.config_file_directory)
    assert os.path.isfile(args.key_file)
    assert os.path.exists(args.output_directory)

    # Set benchmark parameters.
    base_parameters = Parameters(
        config_file_directory=args.config_file_directory,
        f=0,
        num_shards=1,
        key_file=args.key_file,
        num_keys=100 * 1000,
        benchmark_duration_seconds=60,
        transaction_length=5,
        write_percentage=50,
        zipf_coefficient=0,
        num_client_machines=1,
        num_clients_per_machine=1,
        output_directory=args.output_directory,
    )
    parameters_list = [
      base_parameters._replace(
          f=f,
          num_shards=num_shards,
          zipf_coefficient=zipf_coefficient,
          num_client_machines=num_client_machines,
          num_clients_per_machine=num_clients_per_machine,
      )
      for f in [0, 1]
      for num_shards in range(1, 32)
      for zipf_coefficient in [0, 0.5, 0.75, 9]
      for num_client_machines in range(1, 6)
      for num_clients_per_machine in range(1, 32 * 10)
    ]

    # Set remote hosts. Note that the order of the servers below is important.
    # It is assumed that the order matches the order of the hosts in the config
    # files generated by make_config_files.py.
    clients = [
        RemoteHost('10.101.0.13'), # multitapir-client-1
        RemoteHost('10.101.0.14'), # multitapir-client-2
        RemoteHost('10.101.0.10'), # multitapir-client-3
        RemoteHost('10.101.0.12'), # multitapir-client-4
        RemoteHost('10.101.0.11'), # multitapir-client-5
    ]

    servers = [
        RemoteHost('10.101.0.7'), # multitapir-server-1
        RemoteHost('10.101.0.8'), # multitapir-server-2
        RemoteHost('10.101.0.9'), # multitapir-server-3
    ]

    # Run benchmarks.
    for parameters in parameters_list:
        run_benchmark(clients, servers, parameters)

def parser():
    # https://stackoverflow.com/a/4028943/3187068
    home_dir = os.path.expanduser('~')

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config_file_directory',
        type=str,
        default=os.path.join(home_dir, 'tapir_benchmarks'),
        help='The directory in which TAPIR shard files are stored.')
    parser.add_argument(
        '--key_file',
        type=str,
        default=os.path.join(home_dir, 'tapir_benchmarks/keys.txt'),
        help='The TAPIR keys file.')
    parser.add_argument(
        '--output_directory',
        type=str,
        default=os.path.join(home_dir, 'tmp'),
        help='The directory into which benchmark results are written.')
    return parser

if __name__ == '__main__':
    main(parser().parse_args())
