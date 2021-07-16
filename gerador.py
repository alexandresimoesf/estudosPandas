import csv
import os


def gerar_sql():
    for arq in os.listdir('csv'):
        tabela = arq.split('_')[0]
        if tabela == 'prontuarioPermissao':
            tabela = 'permissao_prontuario_clinica'
        arquivo_final = 'sql/' + tabela + '.sql'
        with open('csv/' + arq, 'r', encoding='utf-8') as f:
            linhas = (list(csv.reader(f)))
            colunas = '{}, ' * (len(linhas[0]) - 1) + '{}'
            sql = 'INSERT INTO public.%s (' % tabela + colunas.format(*linhas[0]) + ') VALUES (' + colunas + ');'
            with open(arquivo_final, "a", encoding='utf-8') as arquivo_sql:
                for i in linhas[1:]:
                    print(sql.format(*i), file=arquivo_sql)
