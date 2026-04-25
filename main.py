# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)


# ---------------------------------------------------------------------------
# STEP 1 — Boston employees (firstName, lastName, jobTitle)
# Join employees to offices on officeCode, filter city = 'Boston'
# ---------------------------------------------------------------------------

df_boston = pd.read_sql("""
    SELECT e.firstName, e.lastName
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston'
""", conn)


# ---------------------------------------------------------------------------
# STEP 2 — Offices with zero employees
# LEFT JOIN offices to employees; NULL employeeNumber means no employees
# ---------------------------------------------------------------------------

df_zero_emp = pd.read_sql("""
    SELECT o.officeCode, o.city
    FROM offices o
    LEFT JOIN employees e ON o.officeCode = e.officeCode
    WHERE e.employeeNumber IS NULL
""", conn)


# ---------------------------------------------------------------------------
# STEP 3 — All employees with their office city and state (if they have one)
# LEFT JOIN so employees without an office are still included
# Order by firstName then lastName
# ---------------------------------------------------------------------------

df_employee = pd.read_sql("""
    SELECT e.firstName, e.lastName, o.city, o.state
    FROM employees e
    LEFT JOIN offices o ON e.officeCode = o.officeCode
    ORDER BY e.firstName, e.lastName
""", conn)


# ---------------------------------------------------------------------------
# STEP 4 — Customers who have NOT placed an order
# LEFT JOIN customers to orders; NULL orderNumber means no orders placed
# Return contact info and salesRepEmployeeNumber, sorted by contactLastName
# ---------------------------------------------------------------------------

df_contacts = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName
""", conn)


# ---------------------------------------------------------------------------
# STEP 5 — Customer payments sorted by amount descending
# JOIN customers to payments on customerNumber
# Cast amount to REAL for correct numeric sorting
# ---------------------------------------------------------------------------

df_payment = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
    FROM customers c
    JOIN payments p ON c.customerNumber = p.customerNumber
    ORDER BY CAST(p.amount AS REAL) DESC
""", conn)


# ---------------------------------------------------------------------------
# STEP 6 — Employees whose customers have average credit limit over 90k
# JOIN employees to customers on salesRepEmployeeNumber
# HAVING filters groups after aggregation
# Sort by number of customers high to low
# ---------------------------------------------------------------------------

df_credit = pd.read_sql("""
    SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS num_customers
    FROM employees e
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY e.employeeNumber
    HAVING AVG(CAST(c.creditLimit AS REAL)) > 90000
    ORDER BY num_customers DESC
""", conn)


# ---------------------------------------------------------------------------
# STEP 7 — Products and their order counts and total units sold
# JOIN products to orderdetails on productCode
# GROUP BY product, sort by totalunits DESC
# ---------------------------------------------------------------------------

df_product_sold = pd.read_sql("""
    SELECT p.productName,
           COUNT(od.orderNumber) AS numorders,
           SUM(od.quantityOrdered) AS totalunits
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    GROUP BY p.productCode
    ORDER BY totalunits DESC
""", conn)


# ---------------------------------------------------------------------------
# STEP 8 — Number of distinct customers who ordered each product
# JOIN products → orderdetails → orders → customers
# Use DISTINCT to count unique customers per product
# Sort by numpurchasers DESC
# ---------------------------------------------------------------------------

df_total_customers = pd.read_sql("""
    SELECT p.productName, p.productCode,
           COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY p.productCode
    ORDER BY numpurchasers DESC
""", conn)


# ---------------------------------------------------------------------------
# STEP 9 — Number of customers per office
# JOIN customers → employees → offices
# COUNT customers grouped by office
# ---------------------------------------------------------------------------

df_customers = pd.read_sql("""
    SELECT o.officeCode, o.city, COUNT(c.customerNumber) AS n_customers
    FROM offices o
    JOIN employees e ON o.officeCode = e.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY o.officeCode
""", conn)


# ---------------------------------------------------------------------------
# STEP 10 — Employees who sold products ordered by fewer than 20 customers
# Subquery: find productCodes with fewer than 20 distinct purchasers
# Outer query: find employees linked to those products
# ---------------------------------------------------------------------------

df_under_20 = pd.read_sql("""
    SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders ord ON c.customerNumber = ord.customerNumber
    JOIN orderdetails od ON ord.orderNumber = od.orderNumber
    WHERE od.productCode IN (
        SELECT p.productCode
        FROM products p
        JOIN orderdetails od2 ON p.productCode = od2.productCode
        JOIN orders o2 ON od2.orderNumber = o2.orderNumber
        GROUP BY p.productCode
        HAVING COUNT(DISTINCT o2.customerNumber) < 20
    )
    ORDER BY e.lastName
""", conn)


# ---------------------------------------------------------------------------
# Close the connection
# ---------------------------------------------------------------------------

conn.close()