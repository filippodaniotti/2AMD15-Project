SELECT CONCAT_WS(',', t1._1, t2._2, t3._3) AS full_id, 
       var_udf(agg_udf(ARRAY(t1.ARR, t2.ARR2, t3.ARR3))) AS var
FROM (
    SELECT * FROM (
        SELECT * FROM (
            SELECT _1, ARRAY(_2, ...) AS ARR FROM dataframe) t1 
            CROSS JOIN (
                SELECT _1 AS _2, ARRAY(_2, ...) AS ARR2 FROM dataframe) t2
        WHERE t1._1 < t2._2) t12 
        CROSS JOIN (
            SELECT _1 AS _3, ARRAY(_2, ...) AS ARR3 FROM dataframe) t3
    WHERE t12._2 < t3._3)
WHERE var < τ