package com.yg.scheduler.core;

import com.yg.scheduler.common.JobContext;
import com.yg.scheduler.common.config.AppConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 是任务持久化的数据访问层，负责把任务信息存入 MySQL
 *
 * 保存任务	调度中心下发任务前，把任务信息写入数据库，状态为 PENDING
 * 更新状态	任务执行完成后，更新状态为 SUCCESS、FAILED 或 TIMEOUT
 * 查询任务	调度中心重启时，从数据库恢复未完成的任务
 * 幂等保障	利用 task_id 的唯一索引，防止重复任务插入
 */
public class JobDao {
    private static final Logger log = LoggerFactory.getLogger(JobDao.class);
    private static volatile JobDao instance;
    private final HikariDataSource dataSource;

    JobDao() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(AppConfig.get("db.jdbc-url", "jdbc:mysql://localhost:3306/scheduler?useSSL=false&serverTimezone=UTC"));
        config.setUsername(AppConfig.get("db.username", "root"));
        config.setPassword(AppConfig.get("db.password", "123456"));
        config.setMaximumPoolSize(10);
        config.setConnectionTimeout(5000);
        // 数据库未就绪时不崩 JVM（容器启动时 MySQL 可能还没起来），等首次访问时再重试
        config.setInitializationFailTimeout(-1);
        dataSource = new HikariDataSource(config);
    }

    public static JobDao getInstance() {
        if (instance == null) {
            synchronized (JobDao.class) {
                if (instance == null) {
                    instance = new JobDao();
                }
            }
        }
        return instance;
    }

    public void save(JobContext job, String targetWorker) {
        String sql = "INSERT INTO schedule_job (job_id, task_id,job_name, params, sharding_total, sharding_item ,timeout,target_worker, status,retry_count) VALUES (?, ?, ?, ?, ?, ?, ?,?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, job.getJobId());
            ps.setString(2, job.getTaskId());
            ps.setString(3, job.getJobName());
            ps.setString(4, job.getParams());
            ps.setInt(5, job.getShardingTotal());
            ps.setInt(6, job.getShardingItem());
            ps.setInt(7,job.getTimeout()!=null ? job.getTimeout() :30);
            ps.setString(8, targetWorker);
            ps.setString(9, "PENDING");
            ps.setInt(10,job.getRetryCount()!=null ? job.getRetryCount() : 0);
            ps.executeUpdate();
        } catch (SQLException e) {
            // 唯一索引冲突：任务已存在
            if (e.getErrorCode() == 1062) {
                log.info("Task already exists: {}", job.getTaskId());
            } else {
                log.error("Failed to save job {}", job.getTaskId(), e);
            }
        }
    }

    public void updateStatus(Long jobId, String status, String errorMsg) {
        String sql = "UPDATE schedule_job SET status = ?, error_msg = ? WHERE job_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setString(2, errorMsg);
            ps.setLong(3, jobId);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to update status jobId={} status={}", jobId, status, e);
        }
    }

    public void incrementRetryCount(Long jobId) {
        String sql = "UPDATE schedule_job SET retry_count = retry_count + 1 WHERE job_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, jobId);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to increment retry count jobId={}", jobId, e);
        }
    }

    public JobContext findPendingJobByTaskId(String taskId) {
        String sql = "SELECT * FROM schedule_job WHERE task_id = ? AND status IN ('PENDING', 'RUNNING')";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return JobContext.builder()
                        .jobId(rs.getLong("job_id"))
                        .taskId(rs.getString("task_id"))
                        .jobName(rs.getString("job_name"))
                        .params(rs.getString("params"))
                        .shardingTotal(rs.getInt("sharding_total"))
                        .shardingItem(rs.getInt("sharding_item"))
                        .timeout(rs.getInt("timeout"))
                        .retryCount(rs.getInt("retry_count"))
                        .build();
            }
        } catch (SQLException e) {
            log.error("Failed to find pending job by taskId={}", taskId, e);
        }
        return null;
    }

    public List<JobContext> findPendingJobs() {
        List<JobContext> jobs = new ArrayList<>();
        String sql = "SELECT * FROM schedule_job WHERE status IN ('PENDING', 'RUNNING')";
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                JobContext job = JobContext.builder()
                        .jobId(rs.getLong("job_id"))
                        .taskId(rs.getString("task_id"))
                        .jobName(rs.getString("job_name"))
                        .params(rs.getString("params"))
                        .shardingTotal(rs.getInt("sharding_total"))
                        .shardingItem(rs.getInt("sharding_item"))
                        .timeout(rs.getInt("timeout"))
                        .retryCount(rs.getInt("retry_count"))
                        .build();
                jobs.add(job);
            }
        } catch (SQLException e) {
            log.error("Failed to find pending jobs", e);
        }
        return jobs;
    }
}