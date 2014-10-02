# Spark Integration Tests

This project contains [Docker](http://docker.com)-based integration tests for Spark, including fault-tolerance tests for Spark's standalone cluster manager.

## Requirements

This project depends on Docker >= 1.2.0 (it may work with earlier versions, but this has not been tested recently).

In addition to having Docker, this test suite requires that

- Docker can run without `sudo` (see http://docs.docker.io/en/latest/use/basics/)
- The docker images tagged `spark-test-master` and `spark-test-worker` have been built from the `docker/` directory. Run `docker/spark-test/build` to generate these.
- The `SPARK_HOME` environment variable should to a Spark source checkout where an assembly has been built; this directory will be shared with Docker containers.  Additionally, this Spark sbt project will added as a dependency of this sbt project.

### Requirements for Mesos tests

The Mesos integration tests require `MESOS_NATIVE_LIBRARY` to be set.  For Mac users, the easiest way to install Mesos is through Homebrew:

```
brew install mesos
```

then

```
export MESOS_NATIVE_LIBRARY=$(brew --repository)/lib/libmesos.dylib
```

## Running the tests

These integration tests are implemented as ScalaTest suites and can be run through sbt.  Note that you will probably need to give sbt extra memory; with newer versions of the sbt launcher script, this can be done with the `-mem` option, e.g.

```
sbt -mem 2048
```

*Note:* Although our Docker-based test suites attempt to clean up the containers that they create, this cleanup may not be performed if the test runner's JVM exits abruptly.  To kill **all** Docker containers (including ones that may not have been launched by our tests), you can run `docker kill $(docker ps -q)`.

## Notes for Mac users

On OSX, these integration tests can be run using [boot2docker](https://github.com/boot2docker/boot2docker).  With `boot2docker`, the Docker containers will be run inside of a VirtualBox VM, which creates some difficulties for communication between the Mac host and the containers.

- **Shared volumes**: Our Docker tests rely on being able to mount directories from the Mac host inside of the Docker containers (we use this to share programaticaly-generated Spark configuration directories and the SPARK_HOME directory with the containers).

  This doesn't work with `boot2docker` out of the box (see [boot2docker/534](https://github.com/boot2docker/boot2docker/pull/534) and [docker/7249](https://github.com/docker/docker/issues/7249) for context).
  
  [One workaround](https://gist.github.com/mmerickel/e213fbe7ec7728e4d043#bind-mounting-from-os-x-into-docker-containers) is to use a boot2docker VM that contains the VirtualBox Guest Additions and to mount `/Users` into the VM:
  
  ```
  boot2docker down
  curl http://static.dockerfiles.io/boot2docker-v1.2.0-virtualbox-guest-additions-v4.3.14.iso > ~/.boot2docker/boot2docker.iso
  VBoxManage sharedfolder add boot2docker-vm -name home -hostpath /Users
  boot2docker up
  ``` 
  
- **Network access**:  Our tests currently run the SparkContext from outside of the containers, so we need both host <-> container and container <-> container networking to work properly.  This is complicated by the fact that `boot2docker` runs the containers behind a NAT in VirtualBox.

  [One workaround](http://ispyker.blogspot.com/2014/04/accessing-docker-container-private.html?showComment=1410230338007#c2605390174336905455) is to add a routing table entry that routes traffic to containers to the VirtualBox VM's IP address:
  
  ```
  sudo route -n add 172.17.0.0/16 `boot2docker ip`
  ```
