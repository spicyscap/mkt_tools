SELECT af.distinct_id,
         af.mp_event_name,
         af.mp_insert_id,
         af.time,
         CAST(from_unixtime(af.time,
        'Asia/Taipei') AS DATE) AS time_dt, -- from_unixtime(af.time,'Asia/Taipei') AS time_datetime,
af.new_session_flag, af.page_duration, af.session_id, CONCAT(af.distinct_id, '', CAST(af.time - SUM(af.datetime_flag) OVER(PARTITION BY af.distinct_id, af.session_id
ORDER BY  af.time) AS VARCHAR), '', CAST(af.session_id AS VARCHAR)) AS session_id_full, CAST(from_unixtime((af.time - SUM(af.datetime_flag) OVER(PARTITION BY af.distinct_id, af.session_id
ORDER BY  af.time)),'Asia/Taipei') AS DATE) AS session_dt
FROM 
    (SELECT distinct_id,
         mp_event_name,
         mp_insert_id,
         time,
         new_session_flag,
         page_duration,
         datetime_flag,
         SUM(new_session_flag) OVER(PARTITION BY distinct_id
    ORDER BY  time) AS session_id
    FROM 
        (SELECT distinct_id,
         mp_event_name,
         mp_insert_id,
         time,
         ROW_NUMBER() OVER(PARTITION BY distinct_id
        ORDER BY  time) AS rn, -- 每個人的流水號 (好像可以拿掉?)
        -- [lag series]
        -- LAG(time, 1) OVER(PARTITION BY distinct_id ORDER BY time) AS lag_time,
        -- > （可註解) 往後挪一筆, 計算 Session 用
        -- COALESCE(time - LAG(time, 1) OVER(PARTITION BY distinct_id ORDER BY time),0) AS lag_diff_time,
        -- > （可註解) 跟前一步相減的時間, 若>1800, 表示跟上一頁差了1800秒以上, 是新session的開始, 可以打個mark上去
    IF(time - LAG(time) OVER(PARTITION BY distinct_id
    ORDER BY  time) >= 1800, 1, 0) AS new_session_flag, -- > 若>1800, 表示跟上一頁差了1800秒以上, 是新session的開始, 可以打個mark上去
IF(time - LAG(time) OVER(PARTITION BY distinct_id
ORDER BY  time) >= 1800, 0, COALESCE(time - LAG(time) OVER(PARTITION BY distinct_id
ORDER BY  time),0)) AS datetime_flag, -- > For 標註 Session 起始點用
-- [lead series]
-- LEAD(time, 1) OVER(PARTITION BY distinct_id ORDER BY time) AS lead_time,
-- > （可註解) 往前挪一筆, 計算 停留時間 用
-- COALESCE(LEAD(time,1) OVER(PARTITION BY distinct_id ORDER BY time) -time ,0) AS lead_diff_time,
-- > （可註解) 跟後一步相減的時間, 表示在此event停留的時間, 若>1800, 代表此步已經是最後一步, 應該歸零
IF(LEAD(time,1) OVER(PARTITION BY distinct_id
ORDER BY  time) -time >= 1800, 0, COALESCE(LEAD(time,1) OVER(PARTITION BY distinct_id
ORDER BY  time) -time ,0)) AS page_duration
FROM data_analyze.mixpanel_mp_master_event
WHERE partition_0 = '2020'
        AND partition_1 IN ('01','02','03','04','05')
        AND mp_event_name NOT IN ('SysChk_distinct_id', '$campaign_delivery', '$campaign_received')) AS a0) AS af
