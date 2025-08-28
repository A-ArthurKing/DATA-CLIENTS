USE bp127_ContactRouteReport;
GO

SELECT
    FORMAT(Ast.LocalDateTime, 'yyyy-MM-dd') AS DateJour,
    FORMAT(Ast.LocalDateTime, 'yyyy-MM-dd HH:mm:ss') AS HeureRetrait,
    ad.Account,
    ad.SupGroupKey,
    CONCAT(ad.FirstName, ' ', ad.LastName) AS NomPrenom,
    Ags.Description AS [Retrait],
    p.Description AS [Pause Type],
    FORMAT(DATEADD(SECOND, SUM(Ast.Duration), 0), 'HH:mm:ss') AS Duree
FROM AgentStates Ast
INNER JOIN bp127_admin..Agents ad ON Ast.AgentId = ad.Id
INNER JOIN DIT_AgentState Ags ON Ast.StateId = Ags.Id
LEFT JOIN bp127_admin..Pauses p ON Ast.Data = p.Id
WHERE Ast.LocalDateTime BETWEEN CONVERT(nvarchar(10), GETDATE() - 1, 23) + ' 00:00:00'
    AND CONVERT(nvarchar(10), GETDATE(), 23) + ' 23:59:59'
GROUP BY 
    FORMAT(Ast.LocalDateTime, 'yyyy-MM-dd'),
    FORMAT(Ast.LocalDateTime, 'yyyy-MM-dd HH:mm:ss'),
    ad.Account,
    ad.SupGroupKey,
    CONCAT(ad.FirstName, ' ', ad.LastName),
    Ags.Description,
    p.Description;
GO