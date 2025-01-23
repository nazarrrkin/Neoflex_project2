delete from dm.client
where ctid not in (
	select min(ctid)
	from dm.client
	group by client_rk, effective_from_date
);
