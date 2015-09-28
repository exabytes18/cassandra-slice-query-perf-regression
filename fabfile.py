from fabric.api import put, run, sudo
from fabric.decorators import runs_once, task


@task
@runs_once
def setup_benchmark():
    sudo('mkdir -p /var/lib/cassandra/{data,commitlog,saved_caches}')
    sudo('pip install cassandra-driver')
    run('test -e apache-cassandra-2.0.17 || curl --silent --show-error http://www.us.apache.org/dist/cassandra/2.0.17/apache-cassandra-2.0.17-bin.tar.gz | tar xz')
    run('test -e apache-cassandra-2.1.9 || curl --silent --show-error http://www.us.apache.org/dist/cassandra/2.1.9/apache-cassandra-2.1.9-bin.tar.gz | tar xz')
    put('files/2.0.17-cassandra.yaml', 'apache-cassandra-2.0.17/conf/cassandra.yaml')
    put('files/2.1.9-cassandra.yaml', 'apache-cassandra-2.1.9/conf/cassandra.yaml')
    put('files/benchmark-all.sh', 'benchmark-all.sh', mirror_local_mode=True)
    put('files/benchmark.sh', 'benchmark.sh', mirror_local_mode=True)
    put('files/data_fetch.py', 'data_fetch.py', mirror_local_mode=True)
    put('files/data_gen.py', 'data_gen.py', mirror_local_mode=True)
    put('files/profile.yaml', 'profile.yaml')


@task
@runs_once
def run_benchmark():
    sudo('./benchmark.sh apache-cassandra-2.0.17')
    sudo('./benchmark.sh apache-cassandra-2.1.9')
    run('echo "apache-cassandra-2.0.17: `grep "qps" results/apache-cassandra-2.0.17/fetch.out` qps"')
    run('echo "apache-cassandra-2.1.9: `grep "qps" results/apache-cassandra-2.1.9/fetch.out` qps"')
