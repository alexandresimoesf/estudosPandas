from pandas import read_csv
import csv

arquivo = 'paciente_dermacapelli.csv'
tabela = arquivo.split('_')[0]
arquivo_final = tabela + '.sql'
with open(arquivo, 'r', encoding='utf-8') as f:
    linhas = (list(csv.reader(f)))
    colunas = '{}, ' * (len(linhas[0]) - 1) + '{}'
    sql = 'INSERT INTO public.%s (' % tabela + colunas.format(*linhas[0]) + ') VALUES (' + colunas + ');'
    with open(arquivo_final, "a", encoding='utf-8') as arquivo_sql:
        for i in linhas[1:]:
            print(sql.format(*i), file=arquivo_sql)





