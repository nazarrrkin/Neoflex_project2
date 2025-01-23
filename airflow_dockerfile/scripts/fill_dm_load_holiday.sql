CREATE OR REPLACE PROCEDURE load_holiday_info()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dm.loan_holiday_info
    SELECT d.deal_rk,
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
    FROM rd.deal_info d
    LEFT JOIN rd.loan_holiday lh
           ON d.deal_rk = lh.deal_rk
           AND d.effective_from_date = lh.effective_from_date  --
    LEFT JOIN rd.product p
           ON p.product_rk = d.product_rk
           AND d.effective_from_date = p.effective_from_date ; --
END;
$$;

call load_holiday_info();
