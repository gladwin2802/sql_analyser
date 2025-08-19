SELECT 
    c.CustomerID,
    c.CustomerName,
    o.OrderID,
    o.OrderDate,
    o.TotalAmount,
    RANK() OVER(PARTITION BY c.CustomerID ORDER BY o.OrderDate DESC) AS OrderRank,
    (SELECT SUM(od.Quantity * od.UnitPrice)
     FROM OrderDetails od
     WHERE od.OrderID = o.OrderID) AS OrderItemsTotal
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE o.OrderDate BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY c.CustomerName, o.OrderDate DESC;

SELECT 
    p.ProductID,
    p.ProductName,
    cat.CategoryName,
    COUNT(*) AS TotalOrders,
    AVG(od.UnitPrice) AS AverageUnitPrice,
    SUM(od.Quantity) AS TotalQuantitySold,
    SUM(od.Quantity * od.UnitPrice) AS TotalSales
FROM Products p
JOIN Categories cat ON p.CategoryID = cat.CategoryID
JOIN OrderDetails od ON p.ProductID = od.ProductID
JOIN Orders o ON od.OrderID = o.OrderID
WHERE o.OrderDate >= '2024-06-01'
GROUP BY p.ProductID, p.ProductName, cat.CategoryName
HAVING SUM(od.Quantity) > 100
ORDER BY TotalSales DESC;

SELECT 
    e.EmployeeID,
    e.FirstName,
    e.LastName,
    r.RegionName,
    o.OrderID,
    o.OrderDate,
    o.TotalAmount,
    SUM(o.TotalAmount) OVER(PARTITION BY e.EmployeeID, r.RegionName ORDER BY o.OrderDate 
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CumulativeSales,
    (SELECT AVG(TotalAmount) 
     FROM Orders 
     WHERE EmployeeID = e.EmployeeID) AS AvgEmployeeSales
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.EmployeeID
JOIN Regions r ON o.RegionID = r.RegionID
WHERE o.OrderDate BETWEEN '2024-03-01' AND '2024-09-30'
ORDER