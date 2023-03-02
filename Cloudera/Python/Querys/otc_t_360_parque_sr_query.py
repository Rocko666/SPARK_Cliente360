def qry_otc_t_360_parque_sr(HIVEDB, TABLED):
    qry='''
SELECT 
    num_telefonico AS msisdn 
FROM  {HIVEDB}.{TABLED}
    '''.format(HIVEDB=HIVEDB, TABLED=TABLED)
    return qry