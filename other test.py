matrix = [1,3,4,2]
string_matrix = ''
for number in matrix:
    if matrix.index(number) != len(matrix) - 1:
        string_matrix += '%i-' % number
    else:
        string_matrix += '%i' % number
print string_matrix

