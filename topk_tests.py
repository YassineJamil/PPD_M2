mc_GUI_example = [('S1',1.00,0.065),('S5',0.97,0.175),('S2',0.95,0.738),('S3',0.90,0.191),('S4',0.80,0.175),
                  ('S1',0.69,0.065),('S5',0.58,0.175),('S2',0.42,0.738),('S3',0.38,0.191),('S4',0.14,0.175)]
k = 200
nb_sites = len(mc_GUI_example)
final_result = 0
for j in range(1,nb_sites+1):
    if j != nb_sites:
        temp_result = 0
        for i in range(0, j):
            formula_response = (mc_GUI_example[j-1][1] - mc_GUI_example[j][1])/(mc_GUI_example[i][2])
            temp_result = temp_result + formula_response
            final_result += temp_result
        print final_result
    if final_result >= k:
        break
print j
print final_result
#faire une boucle dans l'index global de 0 a j puis refaire algo du topk normal