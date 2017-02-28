mc_LUI_example = (['1', '1.0'],['2', '0.75'],['3', '0.55'],['4', '0.10'])
for index, t in enumerate(mc_LUI_example) :
    j = index + 1
    if(j == 1) :
        proba_max = float(t[1])
        continue
    current_proba = float(t[1])
    print '(1/%i)*((%f-%f)/(%i-1))' % (len(mc_LUI_example)-1, proba_max, current_proba, j)
    dispersion_measure = (1.0/(len(mc_LUI_example)-1))*((proba_max - current_proba)/float((j-1)))
    print dispersion_measure
# import psycopg2
#
# connexion = psycopg2.connect("dbname='DataWarehouse' user='postgres' host='localhost' password='root'")
# cur = connexion.cursor()
# cur.execute(
#     """
#         SELECT table_name
#         FROM INFORMATION_SCHEMA.TABLES
#         WHERE table_name LIKE \'farm%\';
#     """
# )
# connexion.commit()