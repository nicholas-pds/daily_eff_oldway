SELECT
    cth.CompletedBy,
    CONCAT(EM.FirstName, ' ', EM.LastName) AS [Name],
    ca.CaseNumber,
    cth.CompleteDate,
    ct.Duration
FROM
    dbo.CaseTasksHistory AS cth
INNER JOIN
    dbo.employees AS em
    ON em.EmployeeID = cth.CompletedBy -- This join condition is now correct
INNER JOIN
    dbo.CaseTasks AS ct
    ON ct.CaseID = cth.CaseID
    AND ct.Task = cth.Task
    AND ct.CaseProductID = cth.CaseProductID
INNER JOIN
    dbo.Cases AS ca
    ON ca.CaseID = cth.CaseID
WHERE
    cth.CompleteDate >= DATEADD(day, -5, GETDATE())
    AND cth.Rejected = 0
--AND ca.CaseNumber = '145523'
ORDER BY
    cth.Task ASC;
