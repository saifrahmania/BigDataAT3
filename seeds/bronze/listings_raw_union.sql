-- Combine all split CSVs into one unified logical table
SELECT * FROM {{ ref('listings_raw_part_1') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_2') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_3') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_4') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_5') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_6') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_7') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_8') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_9') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_10') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_11') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_12') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_13') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_14') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_15') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_16') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_17') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_18') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_19') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_20') }}
UNION ALL
SELECT * FROM {{ ref('listings_raw_part_21') }}
