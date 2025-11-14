WITH JobSchedules AS (
    SELECT 
        j.job_id,
        STUFF((
            SELECT DISTINCT ', ' +
                CASE 
                    WHEN s.freq_type = 1 THEN 'One time on ' + CONVERT(VARCHAR(10), s.active_start_date)
                    WHEN s.freq_type = 4 THEN 'Daily'
                    WHEN s.freq_type = 8 THEN 'Weekly'
                    WHEN s.freq_type = 16 THEN 'Monthly (Day ' + CAST(s.freq_interval AS VARCHAR(10)) + ')'
                    WHEN s.freq_type = 32 THEN 'Monthly (Relative)'
                    ELSE 'Other'
                END
            FROM msdb.dbo.sysjobschedules js2
            LEFT JOIN msdb.dbo.sysschedules s ON js2.schedule_id = s.schedule_id
            WHERE js2.job_id = j.job_id
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS ScheduleDays,
        STUFF((
            SELECT DISTINCT ', ' +
                STUFF(STUFF(RIGHT('000000' + CAST(s.active_start_time AS VARCHAR(6)),6),3,0,':'),6,0,':')
            FROM msdb.dbo.sysjobschedules js2
            LEFT JOIN msdb.dbo.sysschedules s ON js2.schedule_id = s.schedule_id
            WHERE js2.job_id = j.job_id
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS ScheduleTimes
    FROM msdb.dbo.sysjobs j
),

LastInstance AS (
    SELECT job_id, MAX(instance_id) AS LastInstanceID
    FROM msdb.dbo.sysjobhistory
    GROUP BY job_id
),

JobRuns AS (
    SELECT 
        j.job_id,
        j.name AS JobName,
        CONCAT(
            STUFF(STUFF(RIGHT('00000000'+CAST(h.run_date AS VARCHAR(8)),8),5,0,'-'),8,0,'-'),
            ' ',
            STUFF(STUFF(RIGHT('000000'+CAST(h.run_time AS VARCHAR(6)),6),3,0,':'),6,0,':')
        ) AS LastRunDateTime,
        h.run_status,
        h.instance_id
    FROM msdb.dbo.sysjobs j
    LEFT JOIN LastInstance li ON j.job_id = li.job_id
    LEFT JOIN msdb.dbo.sysjobhistory h ON h.instance_id = li.LastInstanceID
),

-- NEW: Find the last failed step for each job run
ExactFailure AS (
    SELECT 
        jh.job_id,
        jh.instance_id,
        js.step_name AS FailedStep,
        jh.message AS FailureReason
    FROM msdb.dbo.sysjobhistory jh
    INNER JOIN (
        SELECT job_id, MAX(instance_id) AS instance_id
        FROM msdb.dbo.sysjobhistory
        WHERE run_status = 0 AND step_id > 0
        GROUP BY job_id
    ) latest_fail ON jh.job_id = latest_fail.job_id AND jh.instance_id = latest_fail.instance_id
    LEFT JOIN msdb.dbo.sysjobsteps js ON jh.job_id = js.job_id AND jh.step_id = js.step_id
),

RunningJobs AS (
    SELECT job_id
    FROM msdb.dbo.sysjobactivity
    WHERE start_execution_date IS NOT NULL AND stop_execution_date IS NULL
)

SELECT 
    j.name AS JobName,
    JR.LastRunDateTime,
    CASE 
        WHEN RJ.job_id IS NOT NULL THEN 'Running'
        WHEN JR.run_status = 0 THEN 'Failed'
        WHEN JR.run_status = 1 THEN 'Succeeded'
        WHEN JR.run_status = 4 THEN 'Running'
        ELSE 'Unknown'
    END AS CurrentStatus,
    COALESCE(JS.ScheduleDays,'Not Scheduled') AS ScheduleDays,
    COALESCE(JS.ScheduleTimes,'N/A') AS ScheduleTimes,
    CASE 
        WHEN JR.run_status = 0 THEN 
            'Step: ' + ISNULL(EF.FailedStep,'(Unknown)') + 
            ' | Reason: ' + ISNULL(EF.FailureReason,'No details')
        ELSE EF.FailureReason
    END AS FailureDetails
FROM msdb.dbo.sysjobs j
LEFT JOIN JobSchedules JS ON j.job_id = JS.job_id
LEFT JOIN JobRuns JR ON j.job_id = JR.job_id
LEFT JOIN ExactFailure EF ON JR.job_id = EF.job_id
LEFT JOIN RunningJobs RJ ON j.job_id = RJ.job_id
ORDER BY j.name;
