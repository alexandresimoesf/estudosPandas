from pandas import read_csv
import numpy as np
import unidecode
import re


def nascimento(data):
    return '2020/01/01' if '_' in data else '{}{}{}{}-{}{}-{}{}'.format(*data)


def profissao(servico):
    return 'Null' if servico == 'nan' else unidecode.unidecode(servico)


def quote(informacao):
    return "\'" + informacao + "\'"


def sexo(indefinido):
    return 'MASCULINO' if indefinido == 'M' else 'FEMININO'


def tel(telefone):
    return ''.join(re.findall('\d+', telefone)) if telefone != 'nan' else '99999999'


def remover_lixo(info: str):
    return info.replace("'", '')


pacientes = read_csv('PACIENTE.csv', sep=';', encoding='latin-1', low_memory=False)
pacientes['nascimento'] = pacientes['AnoNascimento'].astype(str) + pacientes['MesDiaNascimento'].astype(str)
pacientes = pacientes.drop(
    ['FotoFicha', 'EnviarMalaDireta', 'NomeUsrMedico', 'FoneIndicacao', 'CodConvenio', 'ValorPagar',
     'ValorPago', 'DtUltimaConsulta', 'DtRetorno', 'HoraMarcada', 'NVisitas', 'UltimoAtendimento',
     'FoneAdicional', 'CodMedico', 'TipoFoneAdicional', 'RecadoCom', 'PacienteInternado',
     'TemFoto', 'NomeFonetico', 'TemInfoClinicas', 'CodigoPrimeiroMedico',
     'EspecialidadeFicha', 'Procedencia', 'Medico', 'Pai', 'TipoFone', 'Conjuge', 'ProfConjuge', 'TemLembrete',
     'TemObs', 'Cor', 'EstadoCivil', 'UltimaAlteracao', 'StatusFinanceiro', 'Filler', 'Naturalidade', 'Endereco',
     'Estado', 'Cidade', 'CEP', 'Mae', 'AnoNascimento', 'MesDiaNascimento'], axis=1)

pacientes['nascimento'] = pacientes['nascimento'].astype(str).apply(nascimento)
pacientes['Sexo'] = pacientes['Sexo'].apply(sexo)
pacientes['Fone'] = pacientes['Fone'].astype(str).apply(tel)
pacientes['DtEntrada'] = pacientes['DtEntrada'].astype(str).apply(nascimento)
pacientes['Profissao'] = pacientes['Profissao'].astype(str).apply(profissao)
pacientes['status_p'] = 'ACTIVED'
pacientes['flagagenda'] = 'False'
pacientes['compartilhar_prontuario'] = 'False'
pacientes['config_cadastro_completo'] = 'Null'
pacientes['cpf_obrigatorio'] = 'False'
pacientes['Nome'] = pacientes['Nome'].astype(str).apply(remover_lixo)
pacientes['ucase_nome'] = pacientes['Nome'].str.upper()

pacientes = pacientes.rename(columns={'CodPaciente': 'id_paciente_dermacapelli',
                                      'Nome': 'nome',
                                      'Fone': 'cel',
                                      'Sexo': 'sexo',
                                      'DtEntrada': 'data_cadastro',
                                      'Profissao': 'profissao'})

q = ['nome', 'data_cadastro', 'sexo', 'cel', 'profissao', 'nascimento', 'ucase_nome', 'status_p']
pacientes[q] = pacientes[q].astype(str).apply(quote)

pacientes.to_csv('paciente_dermacapelli.csv', encoding='UTF8', index=False)
