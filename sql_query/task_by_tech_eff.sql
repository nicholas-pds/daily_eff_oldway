SELECT cth.CompletedBy,
       ca.CaseNumber,
       cth.Task,
       cth.CompleteDate,
       ct.Duration
       --cth.Rejected
FROM dbo.CaseTasksHistory AS cth
INNER JOIN dbo.CaseTasks AS ct ON ct.CaseID = cth.CaseID AND ct.Task = cth.Task AND ct.CaseProductID = cth.CaseProductID
INNER JOIN dbo.Cases as ca ON ca.CaseID = cth.CaseID
WHERE cth.CompleteDate >= DATEADD(day, -5, GETDATE())
AND cth.Rejected = 0
--AND ca.CaseNumber = '145523'
ORDER BY cth.task ASC;