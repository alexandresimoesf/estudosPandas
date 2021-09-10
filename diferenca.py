# import re
# file = 'sql//agenda_leonardo.sql'
# ex = '(.*)(SELECT id FROM public.paciente WHERE paciente.id_paciente_dermacapelli = (.*?) LIMIT 1)'
# with open(file, 'r') as f:
#     for l in f.readlines():
#         print('id_dermacapelli_paciente do médico Leonardo: {}'.format(re.match(ex, l).group(3)), file=open('ids_derma.txt', 'a'))

with open('sql//anamnese.sql', 'r', encoding="latin-1") as f:
    d = set(f.readlines())

with open('anamneseInicio.sql', 'r', encoding="latin-1") as f:
    e = set(f.readlines())

open('sql//anamnese_dif.sql', 'w').close()  # Create the file

with open('sql//anamnese_dif.sql', 'a', encoding="latin-1") as f:
    for line in list(d - e):
        f.write(line)
