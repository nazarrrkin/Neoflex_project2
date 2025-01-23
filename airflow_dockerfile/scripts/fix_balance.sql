create or replace procedure fix_account_balance_out_truth()
as $$
begin
    update rd.account_balance as cur
    set account_in_sum = prev.account_out_sum
    from rd.account_balance as prev
    where cur.account_rk = prev.account_rk
      and cur.effective_date = prev.effective_date + interval '1 day'
      and cur.account_in_sum is distinct from prev.account_out_sum;
end;
$$ language plpgsql;

--выбираем какой-то один из способов

create or replace procedure fix_account_in_truth()
as $$
begin
    update rd.account_balance as prev
    set account_out_sum = cur.account_in_sum
    from rd.account_balance as cur
    WHERE prev.account_rk = cur.account_rk
      and prev.effective_date = cur.effective_date - interval '1 day'
      and prev.account_out_sum is distinct from cur.account_in_sum;
end;
$$ language plpgsql;

create or replace procedure fill_balance_turnover()
as $$
begin
    insert into dm.account_balance_turnover
    select a.account_rk,
        coalesce(dc.currency_name, '-1'::text) as currency_name,
        a.department_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum
    from rd.account a
    left join rd.account_balance ab on a.account_rk = ab.account_rk
    left join dm.dict_currency dc on a.currency_cd = dc.currency_cd;
end;
$$ language plpgsql;

call fill_balance_turnover();
