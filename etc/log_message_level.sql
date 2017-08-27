/*
Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
Description: error navigating and troubleshooting rules
Author: DingQiang Liu
*/

drop table if exists log_message_level;
create table log_message_level(
    name varchar(30),
    table_name varchar(30),
    expression varchar(200)
)
;

drop table if exists issue_category;
create table issue_category(
    cat_id varchar(30) primary key,
    name varchar(30),
    privilege integer,
    table_name varchar(30),
    pattern varchar(200)
)
;

drop table if exists issue_reason;
create table issue_reason(
    reason_id varchar(50) primary key,
    reason_name varchar(50),
    issue_cat_name varchar(30),
    privilege integer,
    table_name varchar(30),
    filter_columns varchar(30),
    reason_pattern varchar(200),
    action varchar(200)
)
;

insert into issue_category values('sql_syntax_error', 'SQL syntax error', 0, 'vertica_log', 'syntax error at or near');

-- rule: down node
insert into log_message_level values('FATAL', 'vertica_log', ' thread_name=''SafetyShutdown'' and message=''Shutting down this node'' ');
insert into issue_category values('node_down', 'node down', 0, 'vertica_log', 'Shutting down this node');
insert into issue_reason values('node_down_network_failed', 'network failed', 'node down', 0, 'vertica_log', 'time', 'message:saw membership message 8192', 'check netowrk interfaces/switch status, and errors/dropped/overrun indictors in output of ifconfig or "ip -s link" commands.');
insert into issue_reason values('node_down_node_left_cluster', 'node left cluster', 'node down', 1, 'vertica_log', 'time', 'message:nodeSetNotifier left the cluster', Null);

-- rule: cluster partitioned
insert into log_message_level values('FATAL', 'vertica_log', ' thread_name=''Spread Client'' and message like ''Cluster partitioned%'' ');
insert into issue_category values('cluster_partitioned', 'cluster partitioned', 0, 'vertica_log', 'Cluster partitioned');
insert into issue_reason values('cluster_partitioned', 'cluster partitioned', 'cluster partitioned', 0, 'vertica_log', 'time', 'message:cluster partitioned', 'check switch status and vlan settings, avoid connecting vertica nodes with different switchs if possible');
insert into issue_reason values('cluster_partitioned_node_left_ksafety', 'node left cluster for ksafety', 'cluster partitioned', 1, 'vertica_log', 'time', 'message:Node left cluster reassessing k safety', 'partitioned cluster cause unsafe nodes down.');
insert into issue_reason values('cluster_partitioned_node_left_unsafe', 'node unsafe', 'cluster partitioned', 2, 'vertica_log', 'time', 'message:Setting node UNSAFE', 'partitioned cluster cause some nodes unsafe.');
insert into issue_reason values('cluster_partitioned_node_left', 'node left cluster', 'cluster partitioned', 3, 'vertica_log', 'time', 'message:nodeSetNotifier left the cluster', 'partitioned cluster cause unsafe nodes down.');

-- rule: NIC link down 
insert into log_message_level values('FATAL', 'messages', ' component=''kernel'' and message like ''%NIC Link is Down%'' ');
insert into issue_category values('nic_link_down', 'NIC Link Down', 1, 'messages', 'NIC Link is Down');
insert into issue_reason values('nic_link_down_down', 'NIC link Down', 'NIC Link Down', 0, 'messages', 'time', 'message:NIC Link is Down', 'please check switch ports logs, cables.');
insert into issue_reason values('nic_link_down_cluster_partitioned', 'cluster partitioned', 'NIC Link Down', 1, 'vertica_log', 'time', 'message:cluster partitioned', 'NIC link down cause cluster partitioned.');
insert into issue_reason values('nic_link_down_cluster_partitioned_node_left_ksafety', 'node left cluster for ksafety', 'NIC Link Down', 2, 'vertica_log', 'time', 'message:Node left cluster reassessing k safety', 'partitioned cluster cause some nodes unsafe.');
insert into issue_reason values('nic_link_down_cluster_partitioned_node_left_unsafe', 'node unsafe', 'NIC Link Down', 3, 'vertica_log', 'time', 'message:Setting node UNSAFE', 'partitioned cluster cause some nodes unsafe.');
insert into issue_reason values('nic_link_down_cluster_partitioned_node_left', 'node left cluster', 'NIC Link Down', 4, 'vertica_log', 'time', 'message:nodeSetNotifier left the cluster', 'partitioned cluster cause unsafe nodes down.');

-- rule: vertica process invoked out of memory killer
insert into log_message_level values('FATAL', 'messages', ' component=''kernel'' and message like ''%vertica invoked oom-killer%'' ');
insert into issue_category values('killed_by_oom', 'killed by OOM', 0, 'messages', 'vertica invoked oom killer');
insert into issue_reason values('killed_by_oom_invoke', 'vertica process invoked out of memory killer', 'killed by OOM', 0, 'messages', 'time', 'message:vertica invoked oom killer', '1. check glibc version, if it is 2.12, it should have a .149 or later suffix. 2. do not run other application on vertica nodes. 3. check your catalog size, if it is over 5% of your memory and your vertica version is under 8.x, please decrease maxmemorysize of your general resource pool.');
insert into issue_reason values('killed_by_oom_killed', 'vertica process was killed' , 'killed by OOM', 1, 'messages', 'time', 'message:Killed process vertica', null);

-- rule: Low disk space detected
insert into log_message_level values('WARNING', 'vertica_log', ' thread_name=''LowDiskSpaceCheck'' and message like ''%Low disk space detected%'' ');
insert into issue_category values('lowdiskspacedetected', 'Low disk space', 0, 'vertica_log', 'Low disk space detected');
insert into issue_reason values('lowdiskspacedetected', 'Low disk space detected', 'Low disk space', 0, 'vertica_log', 'time', 'message:Low disk space detected', 'check disk usage, remove unsed data, or add more disks.');

-- commit;
