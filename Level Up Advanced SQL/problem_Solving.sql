-- New script in level_up_sql.
-- Date: Jan 26, 2024
-- Time: 8:49:20 PM

/*
 * Question 1 : Create a list of employees and their immediate managers
*/

SELECT CONCAT(emp.firstName, ' ', emp.lastName) AS Employeer_Name,
--       emp.firstName AS First_Name,
--       emp.lastName AS Last_Name,
       emp.title AS Employer_Title,
       CONCAT(mng.firstName, ' ', mng.lastName) AS Manager_Name,
       mng.title AS Manager_Title
FROM employee AS emp 
INNER JOIN employee AS mng 
ON emp.managerID = mng.employeeid ;



/* 
 * Question 2 : Find salespeople who have zero sales
*/

SELECT CONCAT(firstname, ' ', lastname) AS emp_name,
       title,
       total_sales
FROM (
    SELECT s.employeeid ,
           e.firstname ,
           e.lastname ,
           e.title ,
           COUNT(s.salesid) AS total_sales
    FROM employee AS e 
    LEFT JOIN sales AS s
    ON s.employeeid = e.employeeid
    WHERE e.title = 'Sales Person'
    GROUP BY s.employeeid , e.firstname , e.lastname, e.title) AS x 
WHERE x.total_sales IS NULL OR x.total_sales = 0;




/*
 * Question 3 : List all customers and their sales, even if some data is gone 
*/

SELECT cus.customerid ,
       CONCAT(cus.firstname, ' ', cus.lastname) AS customer_name,
       cus.city AS city,
       COUNT(s.salesid) AS total_sales
FROM customer AS cus
LEFT JOIN sales AS s 
USING (customerID)
GROUP BY cus.customerid
ORDER BY total_sales DESC;



/*
 * Question 4 : How many cars have been sold per employee?
*/

SELECT emp.employeeid ,
       emp.firstname ,
       emp.lastname ,
       emp.title,
       COUNT(s.salesid) AS total_sales
FROM employee AS emp
LEFT JOIN sales AS s 
USING (employeeID)
WHERE emp.title = 'Sales Person'
GROUP BY 1,2,3,4
ORDER BY total_sales DESC;



/*
 * Question 5 : Find the least and most expensive car sold by each employee
 */

SELECT emp.employeeid ,
       emp.firstname,
       emp.lastname ,
       MAX(s.salesamount) AS highest_valued_sales,
       MIN(s.salesamount) AS least_valued_sales
FROM sales AS s 
INNER JOIN employee AS emp
USING (employeeID)
GROUP BY emp.employeeid ,emp.firstname , emp.lastname;



/*
 * Question 6 : Display a report for employees who have sold more than five cars
 */

SELECT e.employeeid ,
       e.firstname ,
       e.lastname ,
       COUNT(s.salesid) AS total_sales,
       SUM(s.salesamount) AS total_sales_amount
FROM sales AS s 
INNER JOIN employee AS e 
USING (employeeID)
GROUP BY 1,2,3
HAVING COUNT(s.salesid) > 5
ORDER BY total_sales DESC;



/*
 * Question 7 : Summarize sales per year by using a CTE
 */

WITH 
report_cte AS (
    SELECT 
           e.employeeid ,
           CONCAT(e.firstname, ' ', e.lastname) AS emp_name,
           m.modelID ,
           s.salesid,
           m.model,
--           TO_CHAR(s.solddate, 'YYYY') AS year,
           EXTRACT(YEAR FROM s.solddate)::TEXT AS sold_year ,
           s.salesamount 
    FROM sales AS s
    INNER JOIN employee AS e
    USING (employeeID)
    INNER JOIN inventory AS inv 
    USING (inventoryID)
    INNER JOIN model AS m 
    USING (modelID) ),
    
report_cte_lvl_2 AS (
    SELECT emp_name,
           model,
           sold_year,
           COUNT(salesID) AS total_items_sold,
           SUM(salesamount) AS total_sales
    FROM report_cte
    GROUP BY 1,2,3),
    
    
report_cte_empwise AS (
    SELECT emp_name,
           sold_year,
           SUM(total_items_sold) AS total_items_sold,
           SUM(total_sales) AS total_amount
    FROM report_cte_lvl_2
    GROUP BY 1,2
    ORDER BY 1,2 DESC),

    
report_cte_modelwise AS (
    SELECT 
           model,
           sold_year,
           SUM(total_items_sold) AS total_items_sold,
           SUM(total_sales) AS total_amount
    FROM report_cte_lvl_2
    GROUP BY 1,2
    ORDER BY 1,2 DESC
),


report_cte_yearwise AS (
    SELECT 
           sold_year,
           SUM(total_items_sold) AS total_items_sold,
           SUM(total_sales) AS total_amount
    FROM report_cte_lvl_2
    GROUP BY 1
    ORDER BY 1 DESC
)


--SELECT *
--FROM report_cte_empwise;

--SELECT *
--FROM report_cte_modelwise;

SELECT *
FROM report_cte_yearwise;





/*
 * Question 8 : Display the number of sales for each employee by month for 2021
 */


WITH sales_cte AS (
    SELECT e.employeeid ,
           CONCAT(e.firstname, ' ', e.lastname) AS empName,
           s.salesid ,
           s.solddate 
    FROM employee AS e 
    LEFT JOIN sales AS s 
    USING (employeeID)
    WHERE s.solddate BETWEEN '2021-01-01' AND '2021-12-31')

    
SELECT employeeid, 
       empName,
       EXTRACT(MONTH FROM solddate) AS sales_month,
       COUNT(salesID) AS total_sales
FROM sales_cte
GROUP BY 1,2,3
ORDER BY 1,3;



/*
 * Question 9 : Find the sales of cars that are electric by using a subquery
 */

SELECT model,
--       solddate,
       COUNT(x.salesid) AS total_items_sold,
       SUM(salesamount) AS total_amount_of_sales
FROM (
    SELECT s.salesid ,
           m.modelid ,
           m.model ,
           m.enginetype ,
           s.salesamount ,
           s.solddate 
    FROM sales AS s 
    INNER JOIN inventory AS inv 
    USING (inventoryID)
    INNER JOIN model AS m 
    USING (modelID)
    WHERE m.enginetype  = 'Electric') AS x 
GROUP BY 1
ORDER BY total_items_sold DESC, total_amount_of_sales DESC;




/*
 * Question 10 : For each salesperson, rank the car models they've sold the most
 */

WITH emp_most_sold AS (
    SELECT 
           emp.employeeid , 
           CONCAT(emp.firstname, ' ', emp.lastname) AS emp_Name,
    --       emp.title ,
           m.model ,
           COUNT(s.salesid) AS total_cars_sold,
           DENSE_RANK() OVER(PARTITION BY employeeid ORDER BY COUNT(s.salesid) DESC) AS drnk
    FROM employee AS emp
    LEFT JOIN sales AS s
    USING (employeeID)
    INNER JOIN inventory AS inv 
    USING (inventoryID)
    INNER JOIN model AS m 
    USING (modelID)
    WHERE emp.title  = 'Sales Person'
    GROUP BY 1,2,3
    ORDER BY employeeID)

SELECT emp_name,
       model,
       total_cars_sold
FROM emp_most_sold
WHERE drnk = 1
ORDER BY employeeID;



/*
 * Question 11 : Create a report showing sales per month and an annual total
 */

WITH sales_report_1 AS (
    SELECT solddate,
           EXTRACT(YEAR FROM solddate)::TEXT AS sale_year,
           TO_CHAR(solddate, 'Mon') AS sale_month,
           EXTRACT(MONTH FROM solddate) AS sold_month,
           salesid,
           salesamount
    FROM sales ),

sales_report_2 AS (    
    SELECT sale_year,
           sale_month,
           sold_month,
           COUNT(salesid) AS total_sales,
           AVG(salesamount) AS avg_sales_amount,
           SUM(salesamount) AS total_sales_amount
    FROM sales_report_1
    GROUP BY sale_year, sale_month, sold_month
    ORDER BY sale_year, sold_month)

    
    
    
SELECT sale_year,
       sale_month,
       SUM(total_sales) OVER(PARTITION BY sale_year
                             ORDER BY sold_month 
                             ROWS BETWEEN 
                                  UNBOUNDED PRECEDING 
                                AND 
                                  CURRENT ROW) AS total_Sales,
      ROUND(SUM(total_sales_amount) OVER(PARTITION BY sale_year
                             ORDER BY sold_month 
                             ROWS BETWEEN 
                                  UNBOUNDED PRECEDING 
                                AND 
                                  CURRENT ROW),1) AS total_sales_amount
FROM sales_report_2;




/*
 * Question 12 : Display the number of cars sold this month and last month
 */

WITH sales_report AS (
    SELECT salesID,
           solddate ,
           EXTRACT(MONTH FROM solddate) AS sales_month,
           DATE_TRUNC('month', solddate)::DATE AS sold_month
    FROM sales )

SELECT sold_month AS dates,
       COUNT(salesID) AS current_month_sales,
       LAG(COUNT(salesID), 1) OVER(ORDER BY sold_month) AS prev_month_sales
FROM sales_report
GROUP BY sold_month
ORDER BY sold_month;





