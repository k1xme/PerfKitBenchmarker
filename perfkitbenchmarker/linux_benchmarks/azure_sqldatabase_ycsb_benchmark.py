# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run YCSB benchmark against Google Cloud Datastore

Before running this benchmark, you have to download your P12
service account private key file to local machine, and pass the path
via 'google_datastore_keyfile' parameters to PKB.

Service Account email associated with the key file is also needed to
pass to PKB.

By default, this benchmark provision 1 single-CPU VM and spawn 1 thread
to test Datastore.
"""

import posixpath
import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb


BENCHMARK_NAME = 'azure_sqldatabase_ycsb'
BENCHMARK_CONFIG = """
azure_sqldatabase_ycsb:
  description: >
      Run YCSB against relational databases that support JDBC.
      Configure the number of VMs via --num-vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 1"""

YCSB_BINDING_TAR_URL = ('https://github.com/brianfrankcooper/YCSB/releases'
                        '/download/0.9.0/'
                        'ycsb-jdbc-binding-0.9.0.tar.gz')
YCSB_BINDING_LIB_DIR = posixpath.join(ycsb.YCSB_DIR, 'lib')
DRIVER_TAR_URL = ("https://download.microsoft.com/download/"
                   "0/2/A/02AAE597-3865-456C-AE7F-613F99F850A8/"
                   "sqljdbc_4.0.2206.100_enu.tar.gz")
DRIVER_DIR = "/tmp/driver"
FLAGS = flags.FLAGS
flags.DEFINE_string('db_driver',
                    None,
                    'The class of JDBC driver that connects to DB.')
flags.DEFINE_string('db_url',
                    None,
                    'The URL that is used to connect to DB')
flags.DEFINE_string('db_user',
                    None,
                    'The username of target DB.')
flags.DEFINE_string('db_passwd',
                    None,
                    'The password of specified DB user.')
flags.DEFINE_string('driver_tar_url',
                    None,
                    'The password of specified DB user.')


def GetConfig(user_config):
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites():
    # Before YCSB Cloud Datastore supports Application Default Credential,
    # we should always make sure valid credential flags are set.
    if not FLAGS.db_driver:
        raise ValueError('"db_driver" must be set')
    if not FLAGS.db_url:
        raise ValueError('"db_url" must be set')
    if not FLAGS.db_user:
        raise ValueError('"db_user" must be set ')
    if not FLAGS.db_passwd:
        raise ValueError('"db_passwd" must be set ')


def Prepare(benchmark_spec):
    benchmark_spec.always_call_cleanup = True
    default_ycsb_tar_url = ycsb.YCSB_TAR_URL
    vms = benchmark_spec.vms

    # TODO: figure out a less hacky way to override.
    # Override so that we only need to download the required binding.
    ycsb.YCSB_TAR_URL = YCSB_BINDING_TAR_URL

    # Install required packages and copy credential files
    vm_util.RunThreaded(_Install, vms)

    # Restore YCSB_TAR_URL
    ycsb.YCSB_TAR_URL = default_ycsb_tar_url


def Run(benchmark_spec):
    vms = benchmark_spec.vms
    executor = ycsb.YCSBExecutor('jdbc')
    run_kwargs = {
        'db.driver': FLAGS.db_driver,
        'db.url': FLAGS.db_url,
        'db.user': FLAGS.db.db_user,
        'db.passwd': FLAGS.db_passwd,
    }
    load_kwargs = run_kwargs.copy()
    if FLAGS['ycsb_preload_threads'].present:
        load_kwargs['threads'] = FLAGS['ycsb_preload_threads']
    samples = list(executor.LoadAndRun(vms,
                                       load_kwargs=load_kwargs,
                                       run_kwargs=run_kwargs))
    return samples


def Cleanup(benchmark_spec):
    # TODO: support automatic cleanup.
    logging.warning(
        "For now, we can only manually delete all the entries via GCP portal.")


def _Install(vm):
    vm.Install('ycsb')
    
    # Download sqljdbc4.jar.
    vm.RemoteCommand(('mkdir -p {0} && curl -L {1} | '
                      'tar -C {0} --strip-components=1 -xzf && "
                      "cp {0}/*/sqljdbc4.jar {2} -')
                     .format(DRIVER_DIR, DRIVER_TAR_URL, YCSB_BINDING_LIB_DIR))
