from pandas import read_csv
import csv


with open('pacientes_dermacapelli.csv', 'r', encoding='utf-8') as f:
    linhas = (list(csv.reader(f)))
    colunas = '{}, ' * (len(linhas[0]) - 1) + '{}'
    sql = 'INSERT INTO public.paciente (' + colunas.format(*linhas[0]) + ') VALUES (' + colunas + ');'
    for i in linhas[1:5]:
        print(sql.format(*i))





