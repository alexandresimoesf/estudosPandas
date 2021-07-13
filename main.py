from pandas import read_csv
import numpy as np
import unidecode
import re


def nascimento(data):
    return '2020/01/01' if '_' in data else '{}{}{}{}-{}{}-{}{}'.format(*data)


def sexo(indefinido):
    return 'MASCULINO' if indefinido == 'M' else 'FEMININO'


def profissao(servico):
    return 'Null' if servico == 'nan' else unidecode.unidecode(servico)


pacientes = read_csv('PACIENTE_original.csv', sep=';', encoding='latin-1', low_memory=False)
pacientes['nascimento'] = pacientes['AnoNascimento'].astype(str) + pacientes['MesDiaNascimento'].astype(str)
pacientes = pacientes.drop(
    ['FotoFicha', 'EnviarMalaDireta', 'NomeUsrMedico', 'FoneIndicacao', 'CodConvenio', 'ValorPagar',
     'ValorPago', 'DtUltimaConsulta', 'DtRetorno', 'HoraMarcada', 'NVisitas', 'UltimoAtendimento',
     'FoneAdicional', 'TipoFoneAdicional', 'RecadoCom', 'PacienteInternado',
     'TemFoto', 'NomeFonetico', 'TemInfoClinicas', 'CodigoPrimeiroMedico',
     'EspecialidadeFicha', 'Procedencia', 'Medico', 'Pai', 'TipoFone', 'Conjuge', 'ProfConjuge', 'TemLembrete',
     'TemObs', 'Cor', 'EstadoCivil', 'UltimaAlteracao', 'StatusFinanceiro', 'Filler', 'Naturalidade', 'Endereco',
     'Estado', 'Cidade', 'CEP', 'Mae', 'AnoNascimento', 'MesDiaNascimento'], axis=1)


pacientes['nascimento'] = pacientes['nascimento'].astype(str).apply(nascimento)
pacientes['Sexo'] = pacientes['Sexo'].apply(sexo)
pacientes['Fone'] = pacientes['Fone'].astype(str).apply(lambda tel: ''.join(re.findall('\d+', tel)))
pacientes['DtEntrada'] = pacientes['DtEntrada'].astype(str).apply(nascimento)
pacientes['Profissao'] = pacientes['Profissao'].astype(str).apply(profissao)
pacientes['status_p'] = 'ACTIVED'
pacientes['flagagenda'] = 'False'
pacientes['compartilhar_prontuario'] = 'False'
pacientes['config_cadastro_completo'] = 'Null'
pacientes['cpf_obrigatorio'] = 'False'
pacientes['ucase_nome'] = pacientes['Nome'].str.upper()

pacientes = pacientes.rename(columns={'CodPaciente': 'id_paciente_dermacapelli',
                                      'Nome': 'nome',
                                      'Fone': 'cel',
                                      'DtEntrada': 'data_cadastro',
                                      'Profissao': 'profissao'})

pacientes.to_csv('pacientes_dermacapelli.csv', encoding='UTF8', index=False)