WITH "SourceRows" AS (
    SELECT
        T0."DocEntry" AS "GRDocEntry",
        T1."LineNum" AS "GRLineNum",
        T1."ItemCode" AS "ItemCode",
        TO_DECIMAL(IFNULL(T1."Quantity", 0), 19, 6) AS "ReceivedQty",
        TO_DECIMAL(IFNULL(T3."U_DSS_StoreMarkup", 0), 19, 6) AS "U_DSS_StoreMarkup",
        TO_DECIMAL(T2."U_TaxEU", 19, 6) AS "U_TaxEU"
    FROM "KREMMERHUSET"."OPDN" T0
    INNER JOIN "KREMMERHUSET"."PDN1" T1
        ON T0."DocEntry" = T1."DocEntry"
    INNER JOIN "KREMMERHUSET"."OITM" T2
        ON T1."ItemCode" = T2."ItemCode"
    INNER JOIN "KREMMERHUSET"."OITB" T3
        ON T2."ItmsGrpCod" = T3."ItmsGrpCod"
    WHERE
        T0."CreateDate" = TO_DATE('{{TARGET_DATE}}')
        AND T1."WhsCode" = '{{FROM_WAREHOUSE}}'
),
"FactorRows" AS (
    SELECT
        S."GRDocEntry",
        S."GRLineNum",
        S."ItemCode",
        S."ReceivedQty",
        CASE
            WHEN S."U_TaxEU" > 0 THEN S."U_TaxEU"
            ELSE S."U_DSS_StoreMarkup"
        END AS "Factor"
    FROM "SourceRows" S
),
"CalculatedRows" AS (
    SELECT
        F."GRDocEntry",
        F."GRLineNum",
        F."ItemCode",
        F."ReceivedQty",
        F."Factor" AS "FactorUsed",
        TO_DECIMAL(F."ReceivedQty" * F."Factor", 19, 6) AS "CalculatedQty"
    FROM "FactorRows" F
    WHERE IFNULL(F."Factor", 0) > 0
),
"PackSizes" AS (
    SELECT
        C."GRDocEntry",
        C."GRLineNum",
        C."ItemCode",
        C."ReceivedQty",
        C."FactorUsed",
        C."CalculatedQty",
        CASE
            WHEN IFNULL((
                SELECT TO_INT(MAX(IFNULL(S3."BaseQty", 0)))
                FROM "KREMMERHUSET"."ITM12" S2
                INNER JOIN "KREMMERHUSET"."OUOM" S4
                    ON S2."UomEntry" = S4."UomEntry"
                    AND S4."UomCode" LIKE '%%I%%'
                INNER JOIN "KREMMERHUSET"."UGP1" S3
                    ON S4."UomEntry" = S3."UomEntry"
                WHERE
                    S2."ItemCode" = C."ItemCode"
                    AND S2."UomType" = 'P'
            ), 0) <= 0 THEN 1
            ELSE IFNULL((
                SELECT TO_INT(MAX(IFNULL(S3."BaseQty", 0)))
                FROM "KREMMERHUSET"."ITM12" S2
                INNER JOIN "KREMMERHUSET"."OUOM" S4
                    ON S2."UomEntry" = S4."UomEntry"
                    AND S4."UomCode" LIKE '%%I%%'
                INNER JOIN "KREMMERHUSET"."UGP1" S3
                    ON S4."UomEntry" = S3."UomEntry"
                WHERE
                    S2."ItemCode" = C."ItemCode"
                    AND S2."UomType" = 'P'
            ), 1)
        END AS "InnerPackQty",
        CASE
            WHEN IFNULL((
                SELECT TO_INT(MAX(IFNULL(S3."BaseQty", 0)))
                FROM "KREMMERHUSET"."ITM12" S2
                INNER JOIN "KREMMERHUSET"."OUOM" S4
                    ON S2."UomEntry" = S4."UomEntry"
                    AND S4."UomCode" LIKE '%%Y%%'
                INNER JOIN "KREMMERHUSET"."UGP1" S3
                    ON S4."UomEntry" = S3."UomEntry"
                WHERE
                    S2."ItemCode" = C."ItemCode"
                    AND S2."UomType" = 'P'
            ), 0) <= 0 THEN 1
            ELSE IFNULL((
                SELECT TO_INT(MAX(IFNULL(S3."BaseQty", 0)))
                FROM "KREMMERHUSET"."ITM12" S2
                INNER JOIN "KREMMERHUSET"."OUOM" S4
                    ON S2."UomEntry" = S4."UomEntry"
                    AND S4."UomCode" LIKE '%%Y%%'
                INNER JOIN "KREMMERHUSET"."UGP1" S3
                    ON S4."UomEntry" = S3."UomEntry"
                WHERE
                    S2."ItemCode" = C."ItemCode"
                    AND S2."UomType" = 'P'
            ), 1)
        END AS "OuterPackQty"
    FROM "CalculatedRows" C
),
"Rounded" AS (
    SELECT
        P."GRDocEntry",
        P."GRLineNum",
        P."ItemCode",
        P."ReceivedQty",
        P."FactorUsed",
        TO_DECIMAL(
            CASE
                WHEN P."CalculatedQty" <= 0 THEN 0
                WHEN P."InnerPackQty" = 1
                    AND P."OuterPackQty" > 1
                    AND (
                        CEIL(P."CalculatedQty" / NULLIF(P."InnerPackQty", 0)) * P."InnerPackQty" > P."OuterPackQty"
                        OR (
                            CEIL(P."CalculatedQty" / NULLIF(P."InnerPackQty", 0)) * P."InnerPackQty"
                        ) / NULLIF(P."OuterPackQty", 0) > 0.7
                    )
                    THEN CEIL(P."CalculatedQty" / NULLIF(P."OuterPackQty", 0)) * P."OuterPackQty"
                ELSE CEIL(P."CalculatedQty" / NULLIF(P."InnerPackQty", 0)) * P."InnerPackQty"
            END,
            19,
            2
        ) AS "Quantity"
    FROM "PackSizes" P
)
SELECT
    R."ItemCode",
    R."Quantity",
    '{{FROM_WAREHOUSE}}' AS "FromWarehouseCode",
    '{{TO_WAREHOUSE}}' AS "WarehouseCode",
    TO_DECIMAL(R."ReceivedQty", 19, 2) AS "ReceivedQty",
    TO_DECIMAL(R."FactorUsed", 19, 6) AS "FactorUsed"
FROM "Rounded" R
WHERE R."Quantity" > 0
ORDER BY R."GRDocEntry", R."GRLineNum", R."ItemCode";
