select cth.completedby, ca.casenumber, cth.task, cth.completedate, ct.duration
-- cth.Rejected
from dbo.casetaskshistory as cth
inner join
    dbo.casetasks as ct
    on ct.caseid = cth.caseid
    and ct.task = cth.task
    and ct.caseproductid = cth.caseproductid
inner join dbo.cases as ca on ca.caseid = cth.caseid
where cth.completedate >= dateadd(day, -5, getdate()) and cth.rejected = 0
-- AND ca.CaseNumber = '145523'
order by cth.task asc
;
