#!/usr/bin/env python3
# JetBrains PyCharm 2018.1.1
# pyarrow version 0.14.0

import pyarrow as pa
import py3_spark_cassandra_hdfs.config as settings

# connect hadoop distributed file system parameter
# host = settings.hdfs_host
# port = settings.hdfs_port
# user = settings.hdfs_user
# kerb_ticket = settings.hdfs_kerb_ticket

fs = pa.hdfs.connect(settings.hdfs_host, settings.hdfs_port,
                     user=settings.hdfs_user,
                     kerb_ticket=settings.hdfs_kerb_ticket,
                     driver='libhdfs')
