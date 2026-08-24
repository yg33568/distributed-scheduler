package com.yg.scheduler.common.zk;

/**
 * ZooKeeper 路径常量。
 *
 * /scheduler/leader-latch   —— leader 选举 latch（多个 master 竞争）
 * /scheduler/leader         —— 当前 leader 的临时节点，数据 = "host:port"，worker 据此连接
 */
public final class ZkPaths {

    public static final String ROOT = "/scheduler";

    /** LeaderLatch 选举路径 */
    public static final String LEADER_LATCH = ROOT + "/leader-latch";

    /** 当前 leader 地址临时节点（worker 监听它做服务发现） */
    public static final String LEADER = ROOT + "/leader";

    private ZkPaths() {
    }
}
