select sec1.dt as dt, sec1_return, sec2_return
from (
         select dt, ln_return as sec1_return
         from (
                  select dt, LN(close / prv_close) as ln_return
                  from (
                           select dt, close, lag(close) over w as prv_close
                           from sec1_period window w as (order by dt)
                       ) a
              ) b
         where ln_return is not null
         order by dt asc
     )sec1
inner join(
    select dt, ln_return as sec2_return
         from (
                  select dt, LN(close / prv_close) as ln_return
                  from (
                           select dt, close, lag(close) over w as prv_close
                           from sec2_period window w as (order by dt)
                       ) a
              ) b
         where ln_return is not null
         order by dt asc
    )sec2
on sec1.dt = sec2.dt
order by sec1.dt asc;