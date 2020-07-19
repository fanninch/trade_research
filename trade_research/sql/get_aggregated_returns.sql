select *
    , ln(close / prev_close) as ln_prc_chg
from
(
    select withopen.pd_start as pd_start
    , withopen.pd_end   as pd_end
    , withopen.high     as high
    , withopen.low      as low
    , open.open         as open
    , close.close       as close
    , withopen.volume   as volume
    , lag(close, 1) over (order by pd_start) as prev_close
    from (
        select dt, open
        from %REPLACE_TABLE%
    ) open
    inner join(
        select min(agg.pd_start) as pd_start
            , max(agg.dt)       as pd_end
            , min(agg.low)      as low
            , max(agg.high)     as high
            , sum(agg.volume)   as volume
        from (
            select *,
                hour(pd_start)               hour,
                floor(minute(pd_start) / %NUM_MINUTES%) min_pd
            from (
                select dt,
                    subtime(dt, '0 0:1:0') as pd_start,
                    high,
                    low,
                    open,
                    close,
                    volume
                from %REPLACE_TABLE%
                where time(dt) between '08:31:00' and '15:00:00'
            ) a
        ) agg
        group by date(dt), hour, min_pd
    ) withopen
on addtime(withopen.pd_start, '0 0:1:0') = open.dt
inner join(
    select close, dt
    from %REPLACE_TABLE%
    ) close
on withopen.pd_end = close.dt
)final
where prev_close is not null
    and pd_start > '%REPLACE_START_DT%'
    and pd_end < NOW()