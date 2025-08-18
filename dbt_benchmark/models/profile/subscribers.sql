SELECT top 1000 a.pk_g4_subscribers_id, a.p_environment, b.p_ms_orig_src
FROM xyz_dms_cust_986.dbo.e_g4_subscribers a 
left join xyz_dms_cust_986.dbo.e_g4_subscribers_p_ms_orig_src b on a.pk_g4_subscribers_id = b.pk_g4_subscribers_id