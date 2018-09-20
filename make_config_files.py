# A script to generate TAPIR config files.
#
# TAPIR databases are partitioned into shards, and each shard is replicated
# across a number of replicas. When you launch a replica, it has to know which
# other machines are in its replica set. TAPIR accomplishes this by passing the
# replica a config file that includes:  (a) the maximum number of allowable
# failures f and (b) the addresses of all the replicas. A config file looks
# like this:
#
#   f 1
#   replica 10.101.0.7:10000
#   replica 10.101.0.8:10000
#   replica 10.101.0.9:10000
#
# This script creates config files for various values of f and for various
# shards. Every replica in shard i runs on port 10000 + i. For example, here's
# the config file for f = 1 and shard 42:
#
#   f 1
#   replica 10.101.0.7:10042
#   replica 10.101.0.8:10042
#   replica 10.101.0.9:10042
#
# And here's the config file for f = 0 and shard 42:
#
#   f 0
#   replica 10.101.0.7:10042
#
# The config file for f = f and shard = i is named f<f>.shard<i>.config.

def main():
    num_shards = 72
    fs = [0, 1]
    replica_addresses = [
        '10.101.0.7', # multitapir-server-1
        '10.101.0.8', # multitapir-server-2
        '10.101.0.9', # multitapir-server-3
    ]

    for f in fs:
        for shard in range(num_shards):
            config_filename = 'f{}.shard{}.config'.format(f, shard)
            with open(config_filename, 'w') as config_file:
                config_file.write('f {}\n'.format(f))
                for replica in range(2*f + 1):
                    address = replica_addresses[replica]
                    address += ':100{:02}'.format(shard)
                    config_file.write('replica {}\n'.format(address))

if __name__ == '__main__':
   main()
