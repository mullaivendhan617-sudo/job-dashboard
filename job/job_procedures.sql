SELECT 
    s.step_id AS StepID,
    s.step_name AS StepName,
    s.database_name AS DatabaseName,
    CASE 
        WHEN s.subsystem = 'TSQL' THEN 'Transact-SQL'
        WHEN s.subsystem = 'CmdExec' THEN 'Command Line'
        WHEN s.subsystem = 'PowerShell' THEN 'PowerShell Script'
        ELSE s.subsystem
    END AS StepType,
    s.command AS CommandExecuted
FROM msdb.dbo.sysjobsteps s
WHERE s.job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ?)
ORDER BY s.step_id;
