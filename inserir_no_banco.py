import psycopg2

con = psycopg2.connect(host='localhost', database='doctor247', user='postgres', password='123456789')
cur = con.cursor()

# 'agenda.sql', 'prontuario.sql', 'prontuarioPermissao.sql',
for arquivo in ['paciente.sql', 'agenda_ana.sql',
                'agenda_melissa.sql', 'agenda_sofia.sql', 'agenda_leonardo.sql',
                'prontuario_ana.sql', 'prontuario_melissa.sql', 'prontuario_sofia.sql', 'prontuario_leonardo.sql',
                'prontuarioPermissao_ana.sql', 'prontuarioPermissao_melissa.sql', 'prontuarioPermissao_sofia.sql',
                'prontuarioPermissao_leonardo.sql', 'anamnese.sql']:

        arquivo = 'C:\\Users\\Particular\\Desktop\\derma\\' + arquivo
        cur.execute(open(arquivo, 'r', encoding='utf-8').read())
        print(arquivo + ': Enviado')
        con.commit()

con.close()
