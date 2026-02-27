WITH "SourceRows" AS (
    SELECT
        T1."ItemCode" AS "ItemCode",
        TO_DECIMAL(IFNULL(T1."Quantity", 0), 19, 6) AS "ReceivedQty",
        T3."U_DSS_StoreMarkup" AS "U_DSS_StoreMarkup",
        T2."U_TaxEU" AS "U_TaxEU"
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
        S."ItemCode",
        S."ReceivedQty",
        CASE
            WHEN S."U_TaxEU" = 0 THEN NULL
            WHEN S."U_TaxEU" > 0 THEN S."U_TaxEU"
            ELSE IFNULL(S."U_DSS_StoreMarkup", 0)
        END AS "Factor"
    FROM "SourceRows" S
),
"QtyPerItem" AS (
    SELECT
        F."ItemCode",
        SUM(F."ReceivedQty") AS "ReceivedQty",
        SUM(F."ReceivedQty" * F."Factor") AS "CalculatedQty",
        MAX(F."Factor") AS "FactorUsed"
    FROM "FactorRows" F
    WHERE F."Factor" IS NOT NULL
    GROUP BY F."ItemCode"
),
"PackSizes" AS (
    SELECT
        Q."ItemCode",
        Q."ReceivedQty",
        Q."CalculatedQty",
        Q."FactorUsed",
        IFNULL((
            IFNULL((
                SELECT TO_INT(MAX(IFNULL(S3."BaseQty", 0)))
                FROM "KREMMERHUSET"."ITM12" S2
                INNER JOIN "KREMMERHUSET"."OUOM" S4
                    ON S2."UomEntry" = S4."UomEntry"
                    AND S4."UomCode" LIKE '%%I%%'
                INNER JOIN "KREMMERHUSET"."UGP1" S3
                    ON S4."UomEntry" = S3."UomEntry"
                WHERE
                    S2."ItemCode" = Q."ItemCode"
                    AND S2."UomType" = 'P'
            ), 0)
        ), 1) AS "InnerPackQty",
        IFNULL((
            IFNULL((
                SELECT TO_INT(MAX(IFNULL(S3."BaseQty", 0)))
                FROM "KREMMERHUSET"."ITM12" S2
                INNER JOIN "KREMMERHUSET"."OUOM" S4
                    ON S2."UomEntry" = S4."UomEntry"
                    AND S4."UomCode" LIKE '%%Y%%'
                INNER JOIN "KREMMERHUSET"."UGP1" S3
                    ON S4."UomEntry" = S3."UomEntry"
                WHERE
                    S2."ItemCode" = Q."ItemCode"
                    AND S2."UomType" = 'P'
            ), 0)
        ), 1) AS "OuterPackQty"
    FROM "QtyPerItem" Q
),
"Rounded" AS (
    SELECT
        P."ItemCode",
        P."ReceivedQty",
        P."FactorUsed",
        TO_DECIMAL(
            CASE
                WHEN P."CalculatedQty" <= 0 THEN 0
                WHEN P."InnerPackQty" = 1
                    AND P."OuterPackQty" > 1
                    AND (
                        CEIL(P."CalculatedQty" / P."InnerPackQty") * P."InnerPackQty" > P."OuterPackQty"
                        OR (
                            CEIL(P."CalculatedQty" / P."InnerPackQty") * P."InnerPackQty"
                        ) / P."OuterPackQty" > 0.7
                    )
                    THEN CEIL(P."CalculatedQty" / P."OuterPackQty") * P."OuterPackQty"
                ELSE CEIL(P."CalculatedQty" / P."InnerPackQty") * P."InnerPackQty"
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
ORDER BY R."ItemCode";
