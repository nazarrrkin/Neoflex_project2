create or replace procedure load_holiday_info()
as $$
begin
    insert into dm.loan_holiday_info
    select d.deal_rk,
           lh.effective_from_date,
           lh.effective_to_date,
           d.agreement_rk,
           d.account_rk,
           d.client_rk,
           d.department_rk,
           d.product_rk,
           p.product_name,
           d.deal_type_cd,
           d.deal_start_date,
           d.deal_name,
           d.deal_num AS deal_number,
           d.deal_sum,
           lh.loan_holiday_type_cd,
           lh.loan_holiday_start_date,
           lh.loan_holiday_finish_date,
           lh.loan_holiday_fact_finish_date,
           lh.loan_holiday_finish_flg,
           lh.loan_holiday_last_possible_date
    from rd.deal_info d
    left join rd.loan_holiday lh
           on d.deal_rk = lh.deal_rk
           and d.effective_from_date = lh.effective_from_date  --
    left join rd.product p
           on p.product_rk = d.product_rk
           and d.effective_from_date = p.effective_from_date ; --
end;
$$ language plpgsql;

call load_holiday_info();
