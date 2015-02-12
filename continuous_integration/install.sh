#!/bin/bash
# This script is meant to be called by the "install" step defined in
# .travis.yml. See http://docs.travis-ci.com/ for more details.
# The behavior of the script is controlled by environment variabled defined
# in the .travis.yml in the top level folder of the project.

# License: 3-clause BSD


set -xe # Local echo and exit on first error

# Install dependency for full test
pip install coverage coveralls
pip install sphinx_bootstrap_theme


if [[ "$SCHEDULER" == "SLURM" ]]; then
    sudo apt-get install slurm-llnl
    sudo /usr/sbin/create-munge-key
    sudo service munge start
    sudo python continuous_integration/configure_slurm.py

elif [[ "$SCHEDULER" == "SGE" ]]; then
    # The following lines are inspired from the following blog post:
    # http://dan-blanchard.roughdraft.io/6586533-how-to-setup-a-single-machine-sun-grid-engine-installation-for-unit-tests-on-tr
    # The main difference is that we register the real host name as an
    # executor rather than trying to use 'localhost' and the loopback network
    # interface
    export USER=$(id -u -n)
    export CORES=$(grep -c '^processor' /proc/cpuinfo)
    export HOSTNAME=$(hostname)

    sudo apt-get update -qq
    cd continuous_integration/sge
    echo "gridengine-master shared/gridenginemaster string $HOSTNAME" | sudo debconf-set-selections
    echo "gridengine-master shared/gridenginecell string default" | sudo debconf-set-selections
    echo "gridengine-master shared/gridengineconfig boolean true" | sudo debconf-set-selections
    sudo apt-get install gridengine-common gridengine-client gridengine-master
    sudo service gridengine-master restart

    # Install and configure the executor
    sudo apt-get install gridengine-exec

    # Configure the travis worker as a submission host
    sudo qconf -as $HOSTNAME

    # Configure users
    sed -i -r "s/template/$USER/" user_template
    cat user_template
    sudo qconf -Auser user_template
    sudo qconf -au $USER arusers

    # Register the travis host
    sed -i -r "s/localhost/$HOSTNAME/" host_template
    cat host_template
    sudo qconf -Ae host_template

    # Restart the exechost service
    sudo service gridengine-exec restart
    echo "You should see sge_execd and sge_qmaster running below:"
    ps aux | grep "sge"
    echo "The travis worker node should be registered and live:"
    qhost

    # Configure the all.q queue
    sed -i -r "s/localhost/$HOSTNAME/" queue_template
    sed -i -r "s/UNDEFINED/$CORES/" queue_template
    cat queue_template
    sudo qconf -Ap smp_template
    sudo qconf -Aq queue_template
    echo "Printing queue info to verify that things are working correctly."
    qstat -f -q all.q -explain a

    cd ../..

    export SGE_ROOT=/var/lib/gridengine
    export SGE_CELL=default

 fi
