begin;

insert into mart.fact_transactions
  (transaction_id, account_id, client_id, tx_ts, tx_type, amount,
   currency, merchant_category, is_flagged, source_bank, load_batch_date)
select v.tid,
       coalesce(v.acc, a.account_id),
       coalesce(v.cli, a.client_id),
       (date '2026-09-28' + time '12:00'),
       coalesce(v.ttype, t.tx_type),
       v.amt,
       t.currency, t.merchant_category, false, t.source_bank, date '2026-09-28'
from (values
  (9000000000001::bigint, null::bigint, null::bigint, null::text,   -100.00::numeric),  -- DQ-003
  (9000000000002,         null,         null,         'INVALID_TYPE', 50.00),          -- DQ-004
  (9000000000003,         999999999,    null,         null,           50.00),          -- DQ-005
  (9000000000004,         null,         999999999,    null,           50.00)           -- DQ-006
) as v(tid, acc, cli, ttype, amt)
cross join lateral (select account_id, client_id from mart.dim_account order by account_id limit 1) a
cross join lateral (select tx_type, currency, merchant_category, source_bank
                    from mart.fact_transactions
                    where transaction_id < 9000000000000 limit 1) t;

commit;
