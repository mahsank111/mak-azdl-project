-- CRAETING VIEW Customers, FOR GOLD LAYER --


CREATE VIEW goldlayer.customers
AS
SELECT
    *
FROM
    OPENROWSET
        (
        BULK 'https://makstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Customers/',
        FORMAT = 'PARQUET'
        ) as QUER1




-- CRAETING VIEW Calender, FOR GOLD LAYER --


CREATE VIEW goldlayer.calender
AS
SELECT
    *
FROM
    OPENROWSET
        (
        BULK 'https://makstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Calender/',
        FORMAT = 'PARQUET'
        ) as QUER1



-- CRAETING VIEW Products, FOR GOLD LAYER --


CREATE VIEW goldlayer.products
AS
SELECT
    *
FROM
    OPENROWSET
        (
        BULK 'https://makstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Products/',
        FORMAT = 'PARQUET'
        ) as QUER1




-- CRAETING VIEW Returns, FOR GOLD LAYER --


CREATE VIEW goldlayer.returns
AS
SELECT
    *
FROM
    OPENROWSET
        (
        BULK 'https://makstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Returns/',
        FORMAT = 'PARQUET'
        ) as QUER1




-- CRAETING VIEW Sales, FOR GOLD LAYER --


CREATE VIEW goldlayer.sales
AS
SELECT
    *
FROM
    OPENROWSET
        (
        BULK 'https://makstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Sales/',
        FORMAT = 'PARQUET'
        ) as QUER1



-- CRAETING VIEW Subcategories, FOR GOLD LAYER --


CREATE VIEW goldlayer.subcat
AS
SELECT
    *
FROM
    OPENROWSET
        (
        BULK 'https://makstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Subcategories/',
        FORMAT = 'PARQUET'
        ) as QUER1




-- CRAETING VIEW Territories, FOR GOLD LAYER --


CREATE VIEW goldlayer.territories
AS
SELECT
    *
FROM
    OPENROWSET
        (
        BULK 'https://makstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Territories/',
        FORMAT = 'PARQUET'
        ) as QUER1