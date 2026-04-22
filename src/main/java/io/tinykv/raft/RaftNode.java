package io.tinykv.raft;

import io.tinykv.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Raft consensus implementation.
 *
 * <h2>架构概述</h2>
 * Raft 是一种分布式共识算法，将共识问题分解为三个相对独立的子问题：
 * <ol>
 *   <li><b>Leader Election</b> - 选出一个 leader 来管理日志复制</li>
 *   <li><b>Log Replication</b> - leader 接收客户端请求，复制到多数节点后提交</li>
 *   <li><b>Safety</b> - 确保一致性：日志顺序、term 比较、commit 规则</li>
 * </ol>
 *
 * <h2>节点状态</h2>
 * <ul>
 *   <li><b>Follower</b>: 被动节点，响应 leader 和 candidate 的请求</li>
 *   <li><b>Candidate</b>: 正在竞选 leader 的节点</li>
 *   <li><b>Leader</b>: 主节点，处理所有客户端请求</li>
 * </ul>
 *
 * <h2>核心数据结构</h2>
 * <ul>
 *   <li><b>currentTerm</b>: 当前 term，用于识别 stale 请求</li>
 *   <li><b>votedFor</b>: 当前 term 投给的 candidate id</li>
 *   <li><b>log</b>: 日志条目列表，按顺序存储</li>
 *   <li><b>commitIndex</b>: 已提交的日志索引</li>
 *   <li><b>lastApplied</b>: 已应用到状态机的日志索引</li>
 * </ul>
 */
public class RaftNode {

    private static final Logger LOG = LoggerFactory.getLogger(RaftNode.class);

    // ==================== 节点身份 ====================

    /** 节点唯一ID */
    private final int id;

    /** 集群中其他节点ID列表 */
    private final List<Integer> peerIds;

    // ==================== 持久化状态 (所有节点) ====================
    // 这些状态会在每次变更后持久化到磁盘，确保崩溃后能恢复

    /** 当前 term，单调递增 */
    private long currentTerm;

    /** 当前 term 投给的 candidate id，-1 表示没投票 */
    private int votedFor;

    /** Raft 日志，管理所有日志条目 */
    private final RaftLog log;

    // ==================== 易失状态 (所有节点) ====================

    /** 节点当前状态 */
    private volatile RaftState state = RaftState.FOLLOWER;

    /** 已知的当前 leader id，-1 表示未知 */
    private volatile int leaderId = -1;

    /** 已提交的日志索引（可以被 apply 到状态机） */
    private long commitIndex;

    /** 已应用到状态机的日志索引 */
    private long lastApplied;

    // ==================== 易失状态 (仅 Leader) ====================
    // 这些状态仅在 leader 时有意义

    /**
     * nextIndex[i]: leader 下一个要发送给节点 i 的日志索引
     * 初始化为 leader 最后一条日志 + 1
     */
    private long[] nextIndex;

    /**
     * matchIndex[i]: leader 已确认复制到节点 i 的最高日志索引
     * 用于判断是否可以 commit
     */
    private long[] matchIndex;

    // ==================== 选举相关 ====================

    /** 调度器，用于定时任务（选举超时、心跳） */
    private final ScheduledExecutorService scheduler;

    /** 选举超时定时器 */
    private ScheduledFuture<?> electionTimeout;

    /** 随机数生成器，用于随机化选举超时时间 */
    private final Random random = new Random();

    /** 选举超时时间（毫秒） */
    private final int electionTimeoutMs;

    /** 心跳间隔（毫秒） */
    private final int heartbeatIntervalMs;

    // ==================== 状态机和应用 ====================

    /** 状态机接口，用于应用已提交的日志 */
    private final StateMachine stateMachine;

    /** 应用执行器，专门用于 apply 日志到状态机 */
    private final ExecutorService applyExecutor;

    // ==================== 网络传输 ====================

    /** 传输层，用于发送 Raft 消息 */
    private RaftTransport transport;

    // ==================== Learner 模式 ====================
    // Learner 是不参与投票的节点，用于只读副本

    /** 是否为 learner 节点 */
    private final boolean isLearner;

    // ==================== 关闭 ====================

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    // ==================== 构造函数 ====================

    public RaftNode(int id, List<Integer> peerIds, Config config, StateMachine stateMachine) {
        this(id, peerIds, config, stateMachine, false);
    }

    public RaftNode(int id, List<Integer> peerIds, Config config, StateMachine stateMachine, boolean isLearner) {
        this.id = id;
        this.peerIds = peerIds;
        this.stateMachine = stateMachine;
        this.isLearner = isLearner;
        this.electionTimeoutMs = config.getRaftElectionTimeoutMs();
        this.heartbeatIntervalMs = config.getRaftHeartbeatIntervalMs();
        this.log = new RaftLog(config.getDataDir() + "/raft-" + id);

        // 创建调度线程
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-" + id + "-scheduler");
            t.setDaemon(true);
            return t;
        });

        // 创建应用线程
        this.applyExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-" + id + "-apply");
            t.setDaemon(true);
            return t;
        });

        // 初始化 nextIndex/matchIndex 数组，大小能容纳最大节点 ID
        int maxNodeId = id;
        for (int peerId : peerIds) {
            maxNodeId = Math.max(maxNodeId, peerId);
        }
        this.nextIndex = new long[maxNodeId + 1];
        this.matchIndex = new long[maxNodeId + 1];
    }

    // ==================== 启动 ====================

    /**
     * 启动 Raft 节点。
     *
     * 启动流程：
     * 1. 初始化日志，恢复持久化状态
     * 2. 恢复 hard state (term, votedFor, commitIndex)
     * 3. 启动选举超时定时器
     * 4. 启动应用循环
     */
    public void start() throws IOException {
        log.init();

        // 恢复持久化状态
        long[] hs = log.recoverHardState();
        this.currentTerm = hs[0];
        this.votedFor = (int) hs[1];
        this.commitIndex = hs[2];

        LOG.info("Raft node {} starting: term={}, commitIndex={}, state={}", id, currentTerm, commitIndex, state);

        // 启动选举定时器
        resetElectionTimer();

        // 启动应用循环
        startApplyLoop();
    }

    public void setTransport(RaftTransport transport) {
        this.transport = transport;
    }

    // ==================== RPC 消息处理 ====================
    // 这些方法处理来自其他节点的 RPC 请求

    /**
     * 处理 RequestVote RPC。
     *
     * <h3>RequestVote RPC 流程：</h3>
     * <ol>
     *   <li>candidate 的 term > currentTerm：转换为 follower</li>
     *   <li>检查 votedFor：若为空或为 candidate，且 candidate 的日志至少和自己一样新，则投票</li>
     *   <li>"日志足够新" 的判断：(lastLogTerm 更大) 或 (term 相等且 lastLogIndex >= lastLogIndex)</li>
     * </ol>
     *
     * @param req RequestVote 请求
     * @return 投票响应
     */
    public synchronized RaftMessage.RequestVoteResponse handleRequestVote(RaftMessage.RequestVote req) {
        LOG.debug("Node {} received RequestVote from {}: term={}", id, req.from(), req.term());

        // 如果 candidate 的 term 更大，转换为 follower
        if (req.term() > currentTerm) {
            becomeFollower(req.term());
        }

        boolean granted = false;
        if (req.term() >= currentTerm) {
            // 只有在还没投票，或者投给了这个 candidate 时才可能投票
            if (votedFor == -1 || votedFor == req.from()) {
                // 检查 candidate 的日志是否足够新
                // 规则：candidate 的 lastLogTerm > 自己的 lastLogTerm
                //      或 lastLogTerm 相等但 candidate 的 lastLogIndex >= 自己的
                if (req.lastLogTerm() > log.lastLogTerm() ||
                        (req.lastLogTerm() == log.lastLogTerm() && req.lastLogIndex() >= log.lastLogIndex())) {
                    votedFor = req.from();
                    granted = true;
                    persistState();
                    // 重置选举定时器，因为收到了有效的投票请求
                    resetElectionTimer();
                    LOG.info("Node {} voted for {} in term {}", id, req.from(), currentTerm);
                }
            }
        }

        return new RaftMessage.RequestVoteResponse(id, req.from(), currentTerm, granted);
    }

    /**
     * 处理 AppendEntries RPC。
     *
     * <h3>AppendEntries RPC 流程（用于日志复制和心跳）：</h3>
     * <ol>
     *   <li>term < currentTerm：拒绝（stale 请求）</li>
     *   <li>term >= currentTerm：更新为 follower，记录 leader</li>
     *   <li>检查 prevLogIndex/prevLogTerm：确保日志连续性</li>
     *   <li>追加新条目（如果有）</li>
     *   <li>更新 commitIndex（只到 prevLogIndex 或 leaderCommit 的较小值）</li>
     * </ol>
     *
     * <h3>一致性检查：</h3>
     * leader 发送 prevLogIndex 和 prevLogTerm，follower 必须验证：
     * - 如果 prevLogIndex > 0 且 follower 在该位置没有日志，返回 false
     * - 如果有日志但 term 不匹配，返回 false（说明日志分叉了）
     *
     * @param req AppendEntries 请求
     * @return 响应
     */
    public synchronized RaftMessage.AppendEntriesResponse handleAppendEntries(RaftMessage.AppendEntries req) {
        LOG.debug("Node {} received AppendEntries from {}: term={}", id, req.from(), req.term());

        // 拒绝 stale 请求
        if (req.term() < currentTerm) {
            return new RaftMessage.AppendEntriesResponse(id, req.from(), currentTerm, false, 0);
        }

        // 更新为 follower
        if (req.term() >= currentTerm) {
            becomeFollower(req.term());
            leaderId = req.from();
            resetElectionTimer(); // 收到 leader 心跳，重置选举定时器
        }

        // ========== 一致性检查 ==========
        // 检查 prevLogIndex 位置的日志是否存在且 term 匹配
        if (req.prevLogIndex() > 0) {
            // 如果 prevLogIndex 超出本地日志范围
            if (req.prevLogIndex() >= log.size()) {
                return new RaftMessage.AppendEntriesResponse(id, req.from(), currentTerm, false, log.lastLogIndex());
            }
            // 如果 term 不匹配，说明日志分叉了
            if (log.termAt(req.prevLogIndex()) != req.prevLogTerm()) {
                return new RaftMessage.AppendEntriesResponse(id, req.from(), currentTerm, false, log.lastLogIndex());
            }
        }

        // ========== 追加日志条目 ==========
        // 追加新条目（从 prevLogIndex+1 开始）
        List<LogEntry> newEntries = (req.entries() != null) ? Arrays.asList(req.entries()) : List.of();
        boolean success = log.append(req.prevLogIndex(), req.prevLogTerm(), newEntries);

        // ========== 更新 commitIndex ==========
        // 只有 leader 能决定哪些日志可以 commit
        // follower 被动更新自己的 commitIndex
        if (req.leaderCommit() > commitIndex) {
            // 不能超过自己已知的最新日志
            commitIndex = Math.min(req.leaderCommit(), log.lastLogIndex());
            log.setCommitIndex(commitIndex);
        }

        return new RaftMessage.AppendEntriesResponse(id, req.from(), currentTerm, success, log.lastLogIndex());
    }

    // ==================== 客户端请求处理 ====================

    /**
     * 提交一个命令到 Raft 集群（必须在 leader 上调用）。
     *
     * <h3>Propose 流程：</h3>
     * <ol>
     *   <li>将命令包装为 LogEntry，添加到本地日志</li>
     *   <li>更新自己的 matchIndex 和 nextIndex</li>
     *   <li>尝试推进 commitIndex（如果多数节点已复制）</li>
     *   <li>异步复制到所有 follower</li>
     * </ol>
     *
     * @param command 要提交的字节命令
     * @return 新日志条目的索引，-1 表示不是 leader
     */
    public synchronized long propose(byte[] command) {
        if (state != RaftState.LEADER) {
            return -1;
        }

        // 1. 创建日志条目并追加
        LogEntry entry = new LogEntry(currentTerm, log.lastLogIndex() + 1, command);
        long index = log.append(entry);

        // 2. Leader 自身算作已复制
        matchIndex[id] = index;
        nextIndex[id] = index + 1;

        // 3. 尝试推进 commit
        maybeAdvanceCommit();

        // 4. 异步复制到所有 follower
        replicateLog();

        return index;
    }

    /**
     * 提交并等待命令被提交和应用。
     *
     * <h3>ProposeAndWait 流程：</h3>
     * <ol>
     *   <li>调用 propose() 提交命令</li>
     *   <li>轮询等待 lastApplied >= index（命令已被 apply）</li>
     *   <li>超时则抛出异常</li>
     * </ol>
     *
     * @param command 命令字节
     * @param timeoutMs 超时毫秒
     * @return apply 结果（通常为 null）
     */
    public byte[] proposeAndWait(byte[] command, long timeoutMs) throws InterruptedException, TimeoutException {
        long index = propose(command);
        if (index < 0) {
            throw new IllegalStateException("Not the leader");
        }

        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (log.getAppliedIndex() >= index) {
                return null; // 成功
            }
            Thread.sleep(10);
        }
        throw new TimeoutException("Propose timed out after " + timeoutMs + "ms");
    }

    // ==================== Leader Election ====================
    // 领导者选举阶段

    /**
     * 开始一轮选举。
     *
     * <h3>选举流程：</h3>
     * <ol>
     *   <li>转换为 CANDIDATE 状态，term + 1</li>
     *   <li>投票给自己</li>
     *   <li>向所有 peer 发送 RequestVote RPC</li>
     *   <li>收集响应，如果获得多数票则成为 leader</li>
     * </ol>
     *
     * <h3>选举超时：</h3>
     * - Follower 在 electionTimeout 时间内没收到 leader 心跳，就发起选举
     * - 使用随机化超时避免多节点同时选举导致 split vote
     */
    private void startElection() {
        // Learner 不参与选举
        if (isLearner || stopped.get()) return;

        synchronized (this) {
            state = RaftState.CANDIDATE;
            currentTerm++;
            votedFor = id;
            persistState();
            resetElectionVotes(); // 初始化票数（投自己一票）

            LOG.info("Node {} starting election for term {}", id, currentTerm);
        }

        // 收集自己的日志信息
        long lastLogIndex = log.lastLogIndex();
        long lastLogTerm = log.lastLogTerm();

        // 向所有 peer 发送投票请求
        for (int peerId : peerIds) {
            RaftMessage.RequestVote req = new RaftMessage.RequestVote(id, peerId, currentTerm, lastLogIndex, lastLogTerm);
            transport.sendAsync(req, (RaftMessage msg) -> {
                if (msg instanceof RaftMessage.RequestVoteResponse resp) {
                    handleVoteResponse(resp);
                }
            });
        }

        // 单节点集群：直接检查是否获胜
        checkElectionWon();

        // 重置选举定时器
        resetElectionTimer();
    }

    /**
     * 处理投票响应。
     *
     * <h3>处理逻辑：</h3>
     * <ol>
     *   <li>如果状态不是 CANDIDATE，忽略</li>
     *   <li>如果 resp.term > currentTerm：有更新的 term，转换为 follower</li>
     *   <li>如果投票成功，计数并检查是否达到多数</li>
     * </ol>
     *
     * @param resp 投票响应
     */
    private synchronized void handleVoteResponse(RaftMessage.RequestVoteResponse resp) {
        if (state != RaftState.CANDIDATE) return;

        // 发现更新的 term
        if (resp.term() > currentTerm) {
            becomeFollower(resp.term());
            return;
        }

        if (resp.voteGranted()) {
            LOG.info("Node {} received vote from {} in term {}", id, resp.from(), currentTerm);
            // 收到一票，检查是否达到多数
            if (addVoteAndCheckMajority()) {
                becomeLeader();
            }
        }
    }

    // ==================== Log Replication ====================
    // 日志复制阶段

    /**
     * 触发日志复制。
     * Leader 定期调用此方法向所有 follower 发送 AppendEntries。
     *
     * <h3>复制流程：</h3>
     * <ol>
     *   <li>遍历所有 peer</li>
     *   <li>根据 nextIndex[peer] 构建 AppendEntries 请求</li>
     *   <li>包含 prevLogIndex/prevLogTerm 用于一致性检查</li>
     *   <li>异步发送并处理响应</li>
     * </ol>
     */
    private void replicateLog() {
        if (state != RaftState.LEADER) return;

        for (int peerId : peerIds) {
            sendAppendEntries(peerId);
        }
    }

    /**
     * 向指定 peer 发送 AppendEntries RPC。
     *
     * <h3>发送内容：</h3>
     * <ul>
     *   <li>prevLogIndex/prevLogTerm：前一日志的位置信息</li>
     *   <li>entries：要复制的日志条目（从 prevLogIndex+1 开始）</li>
     *   <li>leaderCommit：leader 的 commitIndex</li>
     * </ul>
     *
     * @param peerId 目标节点 ID
     */
    private void sendAppendEntries(int peerId) {
        // 获取下一个要发送的日志索引
        long nextIdx = nextIndex[peerId];
        long prevLogIndex = nextIdx - 1;
        long prevLogTerm = log.termAt(prevLogIndex);

        // 切片要发送的日志（最多 64 条）
        List<LogEntry> entriesToSend = log.slice(nextIdx, Math.min(nextIdx + 64, log.size()));
        LogEntry[] entriesArray = entriesToSend.toArray(new LogEntry[0]);

        RaftMessage.AppendEntries req = new RaftMessage.AppendEntries(
                id, peerId, currentTerm, prevLogIndex, prevLogTerm, entriesArray, commitIndex);

        transport.sendAsync(req, (RaftMessage msg) -> {
            if (msg instanceof RaftMessage.AppendEntriesResponse resp) {
                handleAppendEntriesResponse(resp, peerId);
            }
        });
    }

    /**
     * 处理 AppendEntries 响应。
     *
     * <h3>处理逻辑：</h3>
     * <ol>
     *   <li>如果 success=true：更新 nextIndex 和 matchIndex，尝试推进 commit</li>
     *   <li>如果 success=false：递减 nextIndex 并重试（处理日志不一致）</li>
     * </ol>
     *
     * @param resp AppendEntries 响应
     * @param peerId 来源节点 ID
     */
    private synchronized void handleAppendEntriesResponse(RaftMessage.AppendEntriesResponse resp, int peerId) {
        if (state != RaftState.LEADER) return;

        // 发现更新的 term
        if (resp.term() > currentTerm) {
            becomeFollower(resp.term());
            return;
        }

        if (resp.success()) {
            // 成功：更新 nextIndex 和 matchIndex
            nextIndex[peerId] = resp.matchIndex() + 1;
            matchIndex[peerId] = resp.matchIndex();
            // 尝试推进 commit
            maybeAdvanceCommit();
        } else {
            // 失败：日志不一致，递减 nextIndex 重试
            if (nextIndex[peerId] > 1) {
                nextIndex[peerId]--;
            }
            sendAppendEntries(peerId);
        }
    }

    /**
     * 尝试推进 commitIndex。
     *
     * <h3>Commit 规则（Raft 论文 Figure 8）：</h3>
     * <ul>
     *   <li>Leader 只能 commit 当前 term 的日志</li>
     *   <li>通过计算 matchIndex 来判断是否有多数节点已复制</li>
     *   <li>找到最大的 N，使得 N > commitIndex 且 matchIndex[i] >= N 的节点数 >= majority</li>
     * </ul>
     */
    private void maybeAdvanceCommit() {
        if (state != RaftState.LEADER) return;

        // 遍历所有可能的 commit 位置
        for (long n = commitIndex + 1; n <= log.lastLogIndex(); n++) {
            // 规则：只能 commit 当前 term 的日志（防止 Figure 8 中的问题）
            if (log.termAt(n) != currentTerm) continue;

            // 统计有多少节点已复制到位置 n
            int count = 1; // 自己算一个
            for (int peerId : peerIds) {
                if (matchIndex[peerId] >= n) count++;
            }

            // 检查是否达到多数
            int majority = (peerIds.size() + 1) / 2 + 1;
            if (count >= majority) {
                commitIndex = n;
                log.setCommitIndex(n);
            }
        }
    }

    // ==================== Apply Loop ====================
    // 将已提交的日志应用到状态机

    /**
     * 启动应用循环。
     *
     * <h3>应用流程：</h3>
     * <ol>
     *   <li>持续检查 lastApplied < commitIndex</li>
     *   <li>按顺序应用日志到状态机</li>
     *   <li>更新 lastApplied</li>
     * </ol>
     *
     * <h3>Apply 语义：</h3>
     * - 只 apply commitIndex 之后的日志
     * - 按索引顺序 apply（保证日志顺序）
     * - 幂等性：重复 apply 同一条日志应该安全
     */
    private void startApplyLoop() {
        applyExecutor.submit(() -> {
            while (!stopped.get()) {
                try {
                    // 应用所有已提交但未应用的日志
                    while (lastApplied < commitIndex) {
                        lastApplied++;
                        LogEntry entry = log.get(lastApplied);
                        if (entry != null && entry.data().length > 0) {
                            // 应用到状态机
                            stateMachine.apply(entry.data());
                        }
                        log.setAppliedIndex(lastApplied);
                    }
                    Thread.sleep(1); // 避免空转
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    // ==================== 状态转换 ====================

    /**
     * 转换为 Follower。
     *
     * <h3>转换时机：</h3>
     * <ul>
     *   <li>发现更高的 term</li>
     *   <li>选举超时</li>
     *   <li>收到其他 leader 的 AppendEntries</li>
     * </ul>
     */
    private void becomeFollower(long term) {
        if (currentTerm < term) {
            currentTerm = term;
            votedFor = -1; // 新 term 要重新投票
        }
        state = RaftState.FOLLOWER;
        leaderId = -1;
        persistState();
        cancelElectionTimer();
        resetElectionTimer();
    }

    /**
     * 转换为 Leader。
     *
     * <h3>成为 Leader 后的初始化：</h3>
     * <ol>
     *   <li>设置状态和 leaderId</li>
     *   <li>初始化 nextIndex：所有节点从 lastLogIndex+1 开始</li>
     *   <li>初始化 matchIndex：所有节点从 0 开始</li>
     *   <li>启动心跳</li>
     * </ol>
     */
    private void becomeLeader() {
        state = RaftState.LEADER;
        leaderId = id;
        LOG.info("Node {} became leader for term {}", id, currentTerm);

        // 初始化 nextIndex/matchIndex
        for (int i = 0; i < nextIndex.length; i++) {
            nextIndex[i] = log.lastLogIndex() + 1;
            matchIndex[i] = 0;
        }

        // 启动心跳
        startHeartbeats();
    }

    // ==================== 定时器 ====================

    /**
     * 重置选举定时器。
     *
     * <h3>选举超时机制：</h3>
     * <ul>
     *   <li>超时时间 = electionTimeoutMs + random(0, electionTimeoutMs)</li>
     *   <li>随机化避免多节点同时发起选举</li>
     *   <li>收到 leader 消息时重置</li>
     * </ul>
     */
    private void resetElectionTimer() {
        cancelElectionTimer();
        int timeout = electionTimeoutMs + random.nextInt(electionTimeoutMs);
        electionTimeout = scheduler.schedule(() -> {
            if (state != RaftState.LEADER && !stopped.get()) {
                startElection();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    private void cancelElectionTimer() {
        if (electionTimeout != null) {
            electionTimeout.cancel(false);
        }
    }

    /**
     * 启动心跳。
     * Leader 定期发送心跳阻止 follower 发起选举。
     *
     * <h3>心跳 vs AppendEntries：</h3>
     * <ul>
     *   <li>心跳是 entries=[] 的 AppendEntries</li>
     *   <li>心跳间隔 < 选举超时，确保不触发选举</li>
     *   <li>心跳同时用于推进日志复制</li>
     * </ul>
     */
    private void startHeartbeats() {
        scheduler.scheduleAtFixedRate(() -> {
            if (state == RaftState.LEADER && !stopped.get()) {
                replicateLog();
            }
        }, 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void persistState() {
        log.persistHardState(currentTerm, votedFor, commitIndex);
    }

    // ==================== Getters ====================

    public int getId() { return id; }
    public RaftState getState() { return state; }
    public long getCurrentTerm() { return currentTerm; }
    public int getLeaderId() { return leaderId; }
    public long getCommitIndex() { return commitIndex; }
    public boolean isLeader() { return state == RaftState.LEADER; }
    public boolean isLearner() { return isLearner; }

    // ==================== 关闭 ====================

    public void stop() {
        stopped.set(true);
        cancelElectionTimer();
        scheduler.shutdown();
        applyExecutor.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            applyExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ==================== 票数统计 ====================

    /** 当前选举收到的票数 */
    private int electionVotes = 0;

    private void resetElectionVotes() {
        electionVotes = 1; // 自己的一票
    }

    private boolean addVoteAndCheckMajority() {
        electionVotes++;
        int majority = (peerIds.size() + 1) / 2 + 1;
        return electionVotes >= majority;
    }

    private synchronized void checkElectionWon() {
        if (state == RaftState.CANDIDATE && addVoteAndCheckMajority()) {
            becomeLeader();
        }
    }
}
