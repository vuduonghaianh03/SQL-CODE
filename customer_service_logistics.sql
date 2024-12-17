-- Khám phá database
-- location table
SELECT TOP 5 * FROM [location]
-- order table
SELECT TOP 20 * FROM dbo.orders
-- product table
SELECT TOP 5 * FROM dbo.product
-- review table
SELECT TOP 20 * FROM dbo.review
-- shipper table
SELECT TOP 5 * FROM dbo.shipper


-- Thống kê số lượng số lượng review và trung bình điểm rating theo từng tháng.
WITH table_joined AS (
    SELECT ord.*
    , review_id , score, [description]
    FROM dbo.orders AS ord
    LEFT JOIN dbo.review AS rev
        ON ord.order_id = rev.order_id
)
SELECT [month]
    , COUNT (review_id) AS number_reviews
    , CAST ( AVG (CAST (score AS DECIMAL)) AS DECIMAL (10,2)) AS avg_rating_score
FROM table_joined 
GROUP BY [month]
ORDER BY [month] ASC 


-- Thống kê số lượng số lượng review và trung bình điểm rating theo nhóm sản phẩm.

WITH JoinedOrdersReviews AS (
    SELECT 
        o.order_id,
        r.review_id,
        r.score,
        o.product_id
    FROM
        dbo.orders AS o
    LEFT JOIN dbo.review AS r ON o.order_id = r.order_id
),
JoinedAllTables AS (
    SELECT
        jor.order_id,
        jor.review_id,
        jor.score,
        p.product_name,
        p.category
    FROM
        JoinedOrdersReviews AS jor
    INNER JOIN dbo.product AS p ON jor.product_id = p.product_id
)
SELECT 
    category,
    CAST ( AVG (CAST (score AS DECIMAL)) AS DECIMAL (10,2)) AS avg_cate_rating_score
FROM
    JoinedAllTables
GROUP BY [category]

-- Những lý do nào khiến khách hàng không hài lòng về dịch vụ giao hàng? 

WITH table_joined AS (
    SELECT ord.order_id, [month]
        , review_id , score, [description]
    FROM dbo.orders AS ord
    LEFT JOIN dbo.review AS rev
        ON ord.order_id = rev.order_id
    WHERE [description] IS NOT NULL
)
SELECT description
    , COUNT (order_id) AS number_orders
FROM table_joined
GROUP BY [description]
ORDER BY number_orders DESC

-- Theo thời gian thì lỗi/phản hồi nào biến động? 

WITH table_group AS ( 
SELECT ord.order_id, [month]
        , review_id , score, [description]
        , CASE WHEN score <= 5 THEN 'negative'
               WHEN score <= 7 THEN 'normal'
               ELSE 'positive'
               END AS [description_group]
    FROM dbo.orders AS ord
    LEFT JOIN dbo.review AS rev
        ON ord.order_id = rev.order_id
    WHERE [description] IS NOT NULL
)
SELECT [month], description_group
    , COUNT (description) AS number_of_descriptions
-- hoặc
    , COUNT (order_id) AS number_of_orders
FROM table_group
GROUP BY [month], [description_group]
ORDER BY month ASC 

-- Nhóm sản phẩm nào có nhiều đơn hàng bị phàn nàn "giao hàng chậm" nhiều nhất ?
WITH JoinedOrdersReviews AS (
    SELECT 
        o.order_id,
        r.review_id,
        r.score,
        r.[description],
        o.product_id
    FROM
        dbo.orders AS o
    LEFT JOIN dbo.review AS r ON o.order_id = r.order_id
),
JoinedAllTables AS (
    SELECT
        jor.order_id,
        jor.review_id,
        jor.score,
        jor.[description],
        p.product_name,
        p.category
    FROM
        JoinedOrdersReviews AS jor
    INNER JOIN dbo.product AS p ON jor.product_id = p.product_id
)
SELECT 
    category, description,
    COUNT ([description]) AS number_of_description
FROM JoinedAllTables
WHERE [description] IS NOT NULL
GROUP BY [category], description
ORDER BY [number_of_description] DESC 
    
-- Khu vực nào có điểm rating thấp nhất?
-- Cách 1
WITH JoinedOrdersReviews AS (
    SELECT 
        o.order_id,
        o.location_id,
        r.review_id,
        r.score,
        o.product_id
    FROM
        dbo.orders AS o
    LEFT JOIN dbo.review AS r ON o.order_id = r.order_id
),
JoinedAllTables1 AS (
    SELECT
        jor.order_id,
        jor.review_id,
        jor.score,
        jor.location_id,
        l.economic_region
    FROM
        JoinedOrdersReviews AS jor
    LEFT JOIN dbo.[location] AS l ON jor.location_id = l.location_id
)
SELECT 
    economic_region,
    CAST ( AVG (CAST (score AS DECIMAL)) AS DECIMAL (10,2)) AS avg_region_rating_score
FROM
    JoinedAllTables1
GROUP BY [economic_region]
ORDER BY [avg_region_rating_score] ASC

-- Cách 2: 

WITH table_joined AS ( 
    SELECT o.location_id, o.[month], o.order_id, 
        review_id, province, economic_region, score
    FROM orders AS o
    LEFT JOIN [location] AS l ON o.location_id = l.location_id
    LEFT JOIN review AS r ON o.order_id = r.order_id
)
SELECT economic_region
, CAST (AVG (CAST (score AS DECIMAL)) AS DECIMAL (10,2))AS avg_region_rating_score
FROM table_joined 
GROUP BY economic_region
ORDER BY [avg_region_rating_score] ASC

-- Tính điểm rating theo province
WITH table_joined AS ( 
    SELECT o.location_id, o.[month], o.order_id, 
        review_id, province, score
    FROM orders AS o
    LEFT JOIN [location] AS l ON o.location_id = l.location_id
    LEFT JOIN review AS r ON o.order_id = r.order_id
)
SELECT province
, CAST (AVG (CAST (score AS DECIMAL)) AS DECIMAL (10,2))AS avg_region_rating_score
FROM table_joined 
GROUP BY province
ORDER BY [avg_region_rating_score] ASC

-- Trong năm 2022, tỉnh nào có điểm đánh giá khách hàng cao nhất ?
WITH table_joined AS ( 
    SELECT o.location_id, o.[month], o.order_id, 
        review_id, province, score
    FROM orders AS o
    LEFT JOIN [location] AS l ON o.location_id = l.location_id
    LEFT JOIN review AS r ON o.order_id = r.order_id
)
SELECT province
, CAST (AVG (CAST (score AS DECIMAL)) AS DECIMAL (10,2))AS avg_region_rating_score
FROM table_joined 
WHERE month < '2023-01'
GROUP BY province
ORDER BY [avg_region_rating_score] ASC

-- Khu vực nào có tỷ lệ giao hàng trễ cao nhất?
WITH table_joined AS ( 
    SELECT o.location_id, o.[month], o.order_id,
        status, economic_region
    FROM orders AS o
    LEFT JOIN [location] AS l ON o.location_id = l.location_id
)
, table_total AS (
    SELECT economic_region
    , COUNT (order_id) AS total_orders
    FROM table_joined 
    GROUP BY economic_region
)
, table_late AS (
    SELECT economic_region
    , COUNT (order_id) AS total_late_orders
    FROM table_joined 
    WHERE status = 'late'
    GROUP BY economic_region
)
SELECT table_total.economic_region, table_total.total_orders, total_late_orders
 , CAST (total_late_orders AS DECIMAL) / total_orders AS late_delivery_rate
FROM table_total 
JOIN table_late 
ON table_total.economic_region = table_late.economic_region
ORDER BY [total_late_orders] ASC

-- Tỷ lệ giao hàng trễ theo tỉnh?

WITH table_joined AS ( 
    SELECT o.location_id, o.[month], o.order_id,
        status, economic_region, province
    FROM orders AS o
    LEFT JOIN [location] AS l ON o.location_id = l.location_id
)
, table_total AS (
    SELECT province
    , COUNT (order_id) AS total_orders
    FROM table_joined 
    GROUP BY province
)
, table_late AS (
    SELECT province
    , COUNT (order_id) AS total_late_orders
    FROM table_joined 
    WHERE status = 'late'
    GROUP BY province
)
SELECT table_total.province, table_total.total_orders, total_late_orders
 , CAST (total_late_orders AS DECIMAL) / total_orders AS late_delivery_rate
FROM table_total 
JOIN table_late 
ON table_total.province = table_late.province

-- Extract dataset ra file xlsx (gộp tất cả các bảng lại)
SELECT ord.*
    , province
    , economic_region
    , product_name
    , category
    , review_id
    , score
    , description
    , name AS shipper_name
    , exp_year
FROM dbo.orders AS ord
    LEFT JOIN dbo.[location] AS lo ON ord.location_id = lo.location_id
    LEFT JOIN dbo.product AS pro ON ord.product_id = pro.product_id
    LEFT JOIN dbo.review AS re ON ord.order_id = re.order_id
    LEFT JOIN dbo.shipper AS ship ON ord.shipper_id = ship.shipper_id
