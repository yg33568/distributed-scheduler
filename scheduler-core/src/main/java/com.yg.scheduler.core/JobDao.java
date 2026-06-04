package com.yg.scheduler.core;

import com.yg.scheduler.common.JobContext;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JobDao {
    private static volatile JobDao instance;
    private final HikariDataSource dataSource;

    JobDao() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/scheduler?useSSL=false&serverTimezone=UTC");
        config.setUsername("root");
        config.setPassword("123456");
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
                System.out.println("Task already exists: " + job.getTaskId());
            } else {
                e.printStackTrace();
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
            e.printStackTrace();
        }
    }

    public void incrementRetryCount(Long jobId) {
        String sql = "UPDATE schedule_job SET retry_count = retry_count + 1 WHERE job_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, jobId);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
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
            e.printStackTrace();
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
            e.printStackTrace();
        }
        return jobs;
    }
}